package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	// ==== 新增 gRPC 相关依赖 ====
	"crypto/x509"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	// 引入生成的 proto 包，路径请根据你的 module 名称调整
	proto "solana_compare/proto"
)

type endpointNumType int

const (
	nodeEndpoint1 endpointNumType = iota
	nodeEndpoint2
)

type connType string

const (
	solanaNode  connType = "node"
	blxrGateway connType = "blxr"
	// ==== 新增 grpc 类型 ====
	solanaGrpc connType = "grpc"
)

type solanaSlot struct {
	Params struct {
		Result struct {
			Slot int `json:"slot"`
		} `json:"result"`
	} `json:"params"`
}

func (s *solanaSlot) slotNum(rawBlock []byte) (int, error) {
	err := json.Unmarshal(rawBlock, s)
	if err != nil {
		return -1, err
	}

	return s.Params.Result.Slot, nil
}

type update struct {
	source              string
	rawBlock            []byte
	recvTime            time.Time
	grpcSlot            uint64
	solanaMessageUpdate solanaSlot
}

type entryInfo struct {
	endpointTimes map[string]time.Time
	size          float32
}

func (b *entryInfo) Upd(rt *update) {
	if b.endpointTimes == nil {
		b.endpointTimes = make(map[string]time.Time)
	}

	if _, exists := b.endpointTimes[rt.source]; !exists {
		b.endpointTimes[rt.source] = rt.recvTime
		b.size = float32(len(rt.rawBlock))
	}
}

type wsEndpoint struct {
	connectionType connType
	uri            string
	name           string
}

type wsConnWithEndpointInfo struct {
	conn         *websocket.Conn
	endpointName string
}

var (
	authToken    = flag.String("auth-header", "", "bloxroute authorization header")
	endpointURIs = flag.String(
		"endpoint-ws-uris",
		"",
		"comma separated list of endpoints, "+
			"the endpoint can be either solana node ws endpoint or bloxroute solana services endpoint, "+
			"sample input: blxr+wss://virginia.solana.blxrbdn.com/ws+endpoint1,node+wss://api.mainnet-beta.solana.com+endpoint2",
	)
	blxrSubReq = flag.String(
		"blxr-sub-req",
		`{"id": 1, "method": "subscribe", "params": ["orca", {"include": []}]}`,
		"subscribe request used for bloxroute solana services endpoint",
	)
	seconds     = flag.Int64("interval", 10, "benchmark duration (0 for continuous run)")
	dumpAll     = flag.Bool("dump-all", true, "dump receiving time data to a csv file")
	metricsPort = flag.String("metrics-port", "2112", "prometheus metrics port")
)

// Prometheus metrics
var (
	slotReceiveTimeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solana_slot_receive_time",
			Help: "Receive time for solana slots by endpoint",
		},
		[]string{"slot", "endpoint"},
	)

	slotTimeDifferenceMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solana_slot_time_difference_ms",
			Help: "Time difference in ms between endpoints for the same slot",
		},
		[]string{"slot", "endpoint_fastest", "endpoint_compared"},
	)

	// ==== 新增 gRPC slot 接收时间指标 ====
	grpcSlotReceiveTimeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solana_grpc_slot_receive_time",
			Help: "Receive time for solana slots via gRPC by endpoint",
		},
		[]string{"slot", "endpoint"},
	)

	// ==== gRPC Keepalive 参数 ====
	kacp = keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             time.Second,
		PermitWithoutStream: true,
	}
)

func listen(wg *sync.WaitGroup, ch chan *update, seenMap map[int]*entryInfo) {
	defer wg.Done()
	var firstSeen = func(u *update) *entryInfo {
		entry := &entryInfo{
			endpointTimes: make(map[string]time.Time),
			size:          float32(len(u.rawBlock)),
		}
		entry.endpointTimes[u.source] = u.recvTime
		return entry
	}

	// 保存最近100个slot
	var recentSlots []int
	maxRecentSlots := 100

	// 跟踪每个slot的endpoint
	slotEndpoints := make(map[int]map[string]bool)

	for recv := range ch {
		// 1) 先取 slot
		var slotNum int
		if recv.grpcSlot != 0 {
			slotNum = int(recv.grpcSlot)
		} else {
			n, err := recv.solanaMessageUpdate.slotNum(recv.rawBlock)
			if err != nil || n < 0 {
				continue
			}
			slotNum = n
		}

		// 初始化endpoint跟踪器
		if _, ok := slotEndpoints[slotNum]; !ok {
			slotEndpoints[slotNum] = make(map[string]bool)
		}
		slotEndpoints[slotNum][recv.source] = true

		// 将当前slot添加到最近slots列表
		if !contains(recentSlots, slotNum) {
			recentSlots = append(recentSlots, slotNum)
			// 如果超过100个，移除最早的slot并删除其metrics
			if len(recentSlots) > maxRecentSlots {
				oldestSlot := recentSlots[0]
				recentSlots = recentSlots[1:]

				// 删除最早slot的metrics
				if endpoints, exists := slotEndpoints[oldestSlot]; exists {
					// 删除所有与该slot相关的指标
					slotStr := strconv.Itoa(oldestSlot)

					// 清理receive time metrics
					for endpoint := range endpoints {
						slotReceiveTimeMetric.DeleteLabelValues(slotStr, endpoint)
					}

					// 清理time difference metrics
					for endpoint1 := range endpoints {
						for endpoint2 := range endpoints {
							if endpoint1 != endpoint2 {
								slotTimeDifferenceMetric.DeleteLabelValues(slotStr, endpoint1, endpoint2)
								slotTimeDifferenceMetric.DeleteLabelValues(slotStr, endpoint2, endpoint1)
							}
						}
					}

					// 从跟踪器中移除
					delete(slotEndpoints, oldestSlot)
				}

				// 从seenMap中删除最早的slot
				delete(seenMap, oldestSlot)
			}
		}

		seen, ok := seenMap[slotNum]
		if !ok {
			entry := firstSeen(recv)
			seenMap[slotNum] = entry

			// Record Prometheus metric
			slotReceiveTimeMetric.WithLabelValues(
				strconv.Itoa(slotNum),
				recv.source,
			).Set(float64(recv.recvTime.UnixNano()) / 1e6)

			continue
		}

		seen.Upd(recv)

		// Record Prometheus metric
		slotReceiveTimeMetric.WithLabelValues(
			strconv.Itoa(slotNum),
			recv.source,
		).Set(float64(recv.recvTime.UnixNano()) / 1e6)

		// Calculate and record time differences between endpoints
		var fastestEndpoint string
		var fastestTime time.Time

		// 首先确定最快的endpoint
		for endpoint, timeReceived := range seen.endpointTimes {
			if fastestTime.IsZero() || timeReceived.Before(fastestTime) {
				fastestTime = timeReceived
				fastestEndpoint = endpoint
			}
		}

		// 然后记录所有其他endpoint与最快endpoint的时间差
		for endpoint, timeReceived := range seen.endpointTimes {
			if endpoint == fastestEndpoint {
				continue
			}

			// 计算时间差（总是正值，因为fastestTime一定是最早的）
			timeDifference := timeReceived.Sub(fastestTime).Milliseconds()

			slotTimeDifferenceMetric.WithLabelValues(
				strconv.Itoa(slotNum),
				fastestEndpoint,
				endpoint,
			).Set(float64(timeDifference))
		}
	}
}

// 辅助函数：检查slice中是否包含某个整数
func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func dumpFile(seenMap map[int]*entryInfo, endpoints []wsEndpoint) error {
	csvFile, err := os.Create("BenchmarkOutput.csv")
	log.Printf("Dumping data to csv file BenchmarkOutput.csv")
	if err != nil {
		return err
	}
	defer func(csvFile *os.File) {
		err = csvFile.Close()
		if err != nil {
			log.Errorf("cannot close csv file, %v", err)
		}
	}(csvFile)

	w := csv.NewWriter(csvFile)

	// Create header row with all endpoint names and time differences
	headerRow := []string{"slot"}
	for _, endpoint := range endpoints {
		headerRow = append(headerRow, endpoint.name+" time")
	}

	// Add time difference columns for each endpoint pair
	for i := 0; i < len(endpoints); i++ {
		for j := i + 1; j < len(endpoints); j++ {
			headerRow = append(headerRow, endpoints[i].name+" vs "+endpoints[j].name+" diff")
		}
	}

	if err = w.Write(headerRow); err != nil {
		log.Errorf("error writing record to file, %v\n", err)
	}

	for slotNum, entry := range seenMap {
		row := []string{strconv.Itoa(slotNum)}

		// Add received time for each endpoint
		for _, endpoint := range endpoints {
			timeStr := "not received"
			if t, ok := entry.endpointTimes[endpoint.name]; ok && !t.IsZero() {
				timeStr = t.Format("2006-01-02T15:04:05.000")
			}
			row = append(row, timeStr)
		}

		// Add time differences between each endpoint pair
		for i := 0; i < len(endpoints); i++ {
			for j := i + 1; j < len(endpoints); j++ {
				diffStr := "N/A"
				time1, ok1 := entry.endpointTimes[endpoints[i].name]
				time2, ok2 := entry.endpointTimes[endpoints[j].name]

				if ok1 && ok2 && !time1.IsZero() && !time2.IsZero() {
					diff := time2.Sub(time1)
					diffStr = strconv.FormatInt(diff.Milliseconds(), 10)
				}

				row = append(row, diffStr)
			}
		}

		if err = w.Write(row); err != nil {
			log.Errorf("error writing record to file, %v\n", err)
		}
	}
	w.Flush()
	return err
}

func main() {
	flag.Parse()

	// ==== 注册 Prometheus 指标 ====
	prometheus.MustRegister(slotReceiveTimeMetric)
	prometheus.MustRegister(slotTimeDifferenceMetric)
	prometheus.MustRegister(grpcSlotReceiveTimeMetric)

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Infof("Starting Prometheus metrics server on :%s", *metricsPort)
		if err := http.ListenAndServe(":"+*metricsPort, nil); err != nil {
			log.Fatalf("Error starting Prometheus metrics server: %v", err)
		}
	}()

	endpoints := strings.Split(*endpointURIs, ",")
	if len(endpoints) < 2 {
		log.Fatalln("invalid number of endpoints provided in endpoint-ws-uris, please provide at least two endpoints")
	}

	wsEndpointsList := make([]wsEndpoint, 0)

	for _, typeAndURI := range endpoints {
		parts := strings.Split(typeAndURI, "+")
		if len(parts) != 3 {
			log.Fatalln("invalid endpoint format in endpoint-ws-uris, format should be: type+uri+name")
		}

		connectionTypeStr := parts[0]
		connectionURI := parts[1]
		endpointName := parts[2]

		switch connType(connectionTypeStr) {
		case solanaNode, blxrGateway, solanaGrpc:
		default:
			log.Fatalln("invalid connection type in endpoint-ws-uris")
		}

		if !strings.HasPrefix(connectionURI, "wss:") && !strings.HasPrefix(connectionURI, "ws:") &&
			!strings.HasPrefix(connectionURI, "grpc:") && !strings.HasPrefix(connectionURI, "http") {
			log.Fatalln("invalid protocol in endpoint-ws-uris")
		}

		endpoint := wsEndpoint{
			connectionType: connType(connectionTypeStr),
			uri:            connectionURI,
			name:           endpointName,
		}
		wsEndpointsList = append(wsEndpointsList, endpoint)
	}

	// ==== 先为所有 gRPC 类型端点启动监控 ====
	ch := make(chan *update, 1000)
	for _, e := range wsEndpointsList {
		if e.connectionType == solanaGrpc {
			go monitorGrpc(context.Background(), e.uri, e.name, ch)
		}
	}

	var ctx, cancel = context.WithCancel(context.Background())
	// var ch = make(chan *update, 1000)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		cancel()
		time.Sleep(time.Second)
		if *dumpAll {
			seenSlots := make(map[int]*entryInfo)
			for slotNum, info := range seenSlots {
				if slotNum != 0 && info != nil {
					seenSlots[slotNum] = info
				}
			}

			if err := dumpFile(seenSlots, wsEndpointsList); err != nil {
				log.Errorf("failed creating file %v", err)
			}
		}
		os.Exit(0)
	}()

	var runForever bool
	if *seconds == 0 {
		runForever = true
		log.Printf("The benchmark will run continuously until interrupted")
	} else {
		log.Printf("The benchmark will run for %v seconds", *seconds)
	}

	var readerWG sync.WaitGroup
	// 只为 WebSocket (node/blxr) 类型端点计数，不包括 grpc
	wsCount := 0
	for _, e := range wsEndpointsList {
		if e.connectionType != solanaGrpc {
			wsCount++
		}
	}
	readerWG.Add(wsCount)

	var listenerWG sync.WaitGroup
	listenerWG.Add(1)

	seenSlots := map[int]*entryInfo{}

	connections := make([]wsConnWithEndpointInfo, 0)
	for _, e := range wsEndpointsList {
		// 跳过 grpc 类型，不走 WebSocket 订阅
		if e.connectionType == solanaGrpc {
			continue
		}
		conn := subscribeToFeed(e.uri, e.connectionType)
		connWithName := wsConnWithEndpointInfo{
			conn:         conn,
			endpointName: e.name,
		}
		connections = append(connections, connWithName)
	}

	go listen(&listenerWG, ch, seenSlots)
	for _, c := range connections {
		go read(ctx, &readerWG, ch, c.conn, c.endpointName)
	}

	if !runForever {
		fmt.Println("------------------------------------------------------------------")
		log.Infof("End time: %v", time.Now().Add(time.Duration(time.Second.Nanoseconds()**seconds)).String())

		time.Sleep(time.Duration(*seconds) * time.Second)

		cancel()
		readerWG.Wait()
		close(ch)
		listenerWG.Wait()

		log.Infoln("Streaming finished, processing...")

		printSummary(seenSlots, wsEndpointsList)

		if *dumpAll {
			err := dumpFile(seenSlots, wsEndpointsList)
			if err != nil {
				log.Errorf("failed creating file %v", err)
			}
		}
	} else {
		// Run forever until interrupted
		select {}
	}
}

func printSummary(seenSlots map[int]*entryInfo, endpoints []wsEndpoint) {
	type endpointStats struct {
		totalBlocks    int64
		fasterCount    map[string]int64
		totalDiffNanos map[string]int64
	}

	// Initialize stats for each endpoint
	stats := make(map[string]*endpointStats)
	for _, endpoint := range endpoints {
		stats[endpoint.name] = &endpointStats{
			fasterCount:    make(map[string]int64),
			totalDiffNanos: make(map[string]int64),
		}
	}

	// Count total blocks seen by each endpoint
	totalFromAll := int64(0)
	for _, entry := range seenSlots {
		receivedEndpoints := 0

		for endpointName, timeReceived := range entry.endpointTimes {
			if !timeReceived.IsZero() {
				stats[endpointName].totalBlocks++
				receivedEndpoints++
			}
		}

		if receivedEndpoints >= len(endpoints) {
			totalFromAll++
		}

		// Compare each pair of endpoints
		for i, endpoint1 := range endpoints {
			time1, ok1 := entry.endpointTimes[endpoint1.name]
			if !ok1 || time1.IsZero() {
				continue
			}

			for j, endpoint2 := range endpoints {
				if i == j {
					continue
				}

				time2, ok2 := entry.endpointTimes[endpoint2.name]
				if !ok2 || time2.IsZero() {
					continue
				}

				if time1.Before(time2) {
					stats[endpoint1.name].fasterCount[endpoint2.name]++
					stats[endpoint1.name].totalDiffNanos[endpoint2.name] += time2.Sub(time1).Nanoseconds()
				} else {
					stats[endpoint2.name].fasterCount[endpoint1.name]++
					stats[endpoint2.name].totalDiffNanos[endpoint1.name] += time1.Sub(time2).Nanoseconds()
				}
			}
		}
	}

	fmt.Printf("Summary\n")
	fmt.Printf("Total number of slots seen: %v\n", len(seenSlots))

	for _, endpoint := range endpoints {
		fmt.Printf("Total slots from %s[%s] : %v\n", endpoint.name, endpoint.uri, stats[endpoint.name].totalBlocks)
	}

	fmt.Printf("Total slots received from all endpoints: %v\n", totalFromAll)

	for i, endpoint1 := range endpoints {
		for j, endpoint2 := range endpoints {
			if i >= j {
				continue
			}

			fmt.Printf("Number of slots received first from %s: %v\n",
				endpoint1.name, stats[endpoint1.name].fasterCount[endpoint2.name])
			fmt.Printf("Number of slots received first from %s: %v\n",
				endpoint2.name, stats[endpoint2.name].fasterCount[endpoint1.name])

			totalCompared := stats[endpoint1.name].fasterCount[endpoint2.name] +
				stats[endpoint2.name].fasterCount[endpoint1.name]

			if totalCompared > 0 {
				fmt.Printf("Percentage of slots seen first from %s vs %s: %0.2f\n",
					endpoint2.name, endpoint1.name,
					100*float32(stats[endpoint2.name].fasterCount[endpoint1.name])/float32(totalCompared))

				var endpoint1AverageTimeDif float32
				if stats[endpoint1.name].fasterCount[endpoint2.name] != 0 {
					endpoint1AverageTimeDif = float32(stats[endpoint1.name].totalDiffNanos[endpoint2.name]/1e6) /
						float32(stats[endpoint1.name].fasterCount[endpoint2.name])
				}

				var endpoint2AverageTimeDif float32
				if stats[endpoint2.name].fasterCount[endpoint1.name] != 0 {
					endpoint2AverageTimeDif = float32(stats[endpoint2.name].totalDiffNanos[endpoint1.name]/1e6) /
						float32(stats[endpoint2.name].fasterCount[endpoint1.name])
				}

				fmt.Printf("The average time difference for slots received first from %s (ms): %0.2f\n",
					endpoint1.name, endpoint1AverageTimeDif)
				fmt.Printf("The average time difference for slots received first from %s (ms): %0.2f\n",
					endpoint2.name, endpoint2AverageTimeDif)

				overallDiff := (float32(stats[endpoint2.name].fasterCount[endpoint1.name])*endpoint2AverageTimeDif -
					float32(stats[endpoint1.name].fasterCount[endpoint2.name])*endpoint1AverageTimeDif) /
					float32(totalCompared)

				if overallDiff > 0 {
					fmt.Printf("On average, %s is faster than %s with a time difference (ms): %0.2f\n",
						endpoint2.name, endpoint1.name, overallDiff)
				} else {
					fmt.Printf("On average, %s is faster than %s with a time difference (ms): %0.2f\n",
						endpoint1.name, endpoint2.name, -overallDiff)
				}
			}

			fmt.Println()
		}
	}
}

func read(
	ctx context.Context,
	wg *sync.WaitGroup,
	ch chan *update,
	c *websocket.Conn,
	endpointName string,
) {
	defer wg.Done()
	defer closeConnection(c)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, r, err := c.NextReader()
			if err != nil {
				log.Errorf("next reader for %s, %v\n", endpointName, err)
				continue
			}

			message, err := ioutil.ReadAll(r)
			if err != nil {
				log.Errorf("read error for %s, %v\n", endpointName, err)
				continue
			}

			ch <- &update{
				source:              endpointName,
				rawBlock:            message,
				recvTime:            time.Now(),
				grpcSlot:            0,
				solanaMessageUpdate: solanaSlot{},
			}
		}
	}
}

func subscribeToFeed(connectionURI string, endpointType connType) *websocket.Conn {
	header := http.Header{}
	header.Add("x-infra-biz", "sintral")
	var requestBody string

	if endpointType == blxrGateway {
		header.Add("Authorization", *authToken)
		requestBody = *blxrSubReq
	} else {
		requestBody = `{"id": 1, "jsonrpc":"2.0", "method": "slotSubscribe"}`
	}

	tlsConfig := tls.Config{}
	if strings.HasPrefix(connectionURI, "wss") {
		tlsConfig.InsecureSkipVerify = true
	}
	dialer := websocket.Dialer{TLSClientConfig: &tlsConfig}

	log.Printf("connecting to %s", connectionURI)
	c, resp, err := dialer.Dial(connectionURI, header)
	if err != nil {
		log.Fatalln("dial error, %v", err)
	}
	defer resp.Body.Close()

	// subscribe to feed
	err = c.WriteMessage(websocket.TextMessage, []byte(requestBody))
	if err != nil {
		log.Fatalln("failed to write message:", err)
	}

	_, response, err := c.ReadMessage()
	if err != nil {
		log.Fatalln(err)
	}

	err = checkThatConnectionSuccessfullyEstablished(response)
	if err != nil {
		log.Fatalln(err)
	}

	return c
}

func closeConnection(conn *websocket.Conn) {
	err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.Errorf("failed write close message to socket: %s", err)
	}
	err = conn.Close()
	if err != nil {
		log.Errorf("failed to close connection: %s", err)
	}
}

func checkThatConnectionSuccessfullyEstablished(response []byte) error {
	var rpcResponse map[string]interface{}
	err := json.Unmarshal(response, &rpcResponse)
	if err != nil {
		return err
	}

	rpcError, ok := rpcResponse["error"]
	if !ok {
		return nil
	}

	return fmt.Errorf("error from RPC: %v", rpcError)
}

// ==== 新增：根据 Scheme 生成 gRPC DialOption 列表 ====
func getGrpcDialOptions(insecureConn bool) []grpc.DialOption {
	var opts []grpc.DialOption
	if insecureConn {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	opts = append(opts, grpc.WithKeepaliveParams(kacp))
	return opts
}

// ==== 新增：gRPC 订阅函数，持续拉取 slot 更新并上报 Prometheus ====
func monitorGrpc(ctx context.Context, connectionURI, endpointName string, ch chan *update) {
	u, err := url.Parse(connectionURI)
	if err != nil {
		log.Errorf("invalid grpc URI %s: %v", connectionURI, err)
		return
	}

	// 根据 Scheme 判断是否使用 TLS
	insecureConn := u.Scheme != "https"
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if insecureConn {
			port = "80"
		} else {
			port = "443"
		}
	}
	address := host + ":" + port

	// 建立 gRPC 连接
	conn, err := grpc.DialContext(ctx, address, getGrpcDialOptions(insecureConn)...)
	if err != nil {
		log.Errorf("grpc dial error for %s: %v", endpointName, err)
		return
	}
	defer conn.Close()

	client := proto.NewGeyserClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Errorf("grpc subscribe error for %s: %v", endpointName, err)
		return
	}

	// 构造订阅请求，只关注 slot 更新
	filter := true
	req := &proto.SubscribeRequest{
		Commitment: proto.CommitmentLevel(0).Enum(),
		Slots: map[string]*proto.SubscribeRequestFilterSlots{
			"incoming_slots": {FilterByCommitment: &filter},
		},
		Accounts:          make(map[string]*proto.SubscribeRequestFilterAccounts),
		Transactions:      make(map[string]*proto.SubscribeRequestFilterTransactions),
		Blocks:            make(map[string]*proto.SubscribeRequestFilterBlocks),
		BlocksMeta:        make(map[string]*proto.SubscribeRequestFilterBlocksMeta),
		AccountsDataSlice: []*proto.SubscribeRequestAccountsDataSlice{},
	}
	if err := stream.Send(req); err != nil {
		log.Errorf("grpc send subscribe for %s: %v", endpointName, err)
		return
	}

	// 持续接收更新并上报指标
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Errorf("grpc recv error for %s: %v", endpointName, err)
			time.Sleep(5 * time.Second)
			continue
		}
		slotNum := msg.GetSlot().GetSlot()
		now := time.Now()
		ch <- &update{
			source:   endpointName,
			recvTime: now,
			grpcSlot: slotNum,
		}
	}
}
