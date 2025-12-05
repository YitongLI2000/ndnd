package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
)

const (
	INIT_INTEREST_RATE    = 2000.0                 // Initial Rate: 2000 interests/sec
	TOTAL_DURATION        = 30 * time.Second       // Max duration (hard stop)
	MAX_PACKETS           = 30000                  // Target packets to receive per flow
	INTEREST_LIFETIME     = 4 * time.Second        // Individual Interest lifetime
	RTT_WINDOW_DURATION   = 1 * time.Second        // Duration to keep RTT samples for RTO
	MIN_RTO               = 30.0                   // Minimum RTO in milliseconds
	MAX_RTO               = 100.0                  // Maximum RTO in milliseconds (Cap)
	THROUGHPUT_WINDOW_DUR = 100 * time.Millisecond // Sliding window size for throughput/RTT
	CHECK_INTERVAL        = 10 * time.Millisecond  // Retransmission check interval (Lowered for tighter control)
)

// Create a tag for our consumer application
type ConsumerTag struct{}

func (ConsumerTag) String() string {
	return "NDN-Consumer"
}

var consumerTag = ConsumerTag{}

// PacketSample helps track throughput and RTT in the sliding window
type PacketSample struct {
	Timestamp time.Time
	Size      int
	RTT       float64 // in milliseconds
}

// FlowContext holds state specific to a single flow (target prefix)
type FlowContext struct {
	mu     sync.Mutex
	Prefix string

	// Statistics
	totalBytes        int64
	pktsReceivedTotal int // Total valid Data packets received
	startTime         time.Time
	endTime           time.Time
	isFinished        bool
	doneCh            chan struct{} // Signal channel when MAX_PACKETS reached

	// Unified Sliding Window (100ms)
	window []PacketSample

	// Rate Control State
	currentRate      float64       // Interests per second
	pktsCalibrated   int           // Count for initial calibration (distinct from total)
	initialRTTSum    float64       // Sum of first 5 RTTs
	baseRTT          float64       // Average of first 5 RTTs (Baseline)
	adjustmentPeriod time.Duration // Calculated from first 5 packets
	lastRateUpdate   time.Time     // Last time we adjusted the rate
}

func NewFlowContext(prefix string) *FlowContext {
	now := time.Now()
	return &FlowContext{
		Prefix:         prefix,
		startTime:      now,
		endTime:        now,
		window:         make([]PacketSample, 0),
		currentRate:    INIT_INTEREST_RATE,
		pktsCalibrated: 0,
		lastRateUpdate: now,
		doneCh:         make(chan struct{}),
	}
}

// GetRate returns the current dynamic sending rate
func (f *FlowContext) GetRate() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentRate
}

// RecordPacket updates window, stats, and adjusts sending rate based on RTT congestion control
func (f *FlowContext) RecordPacket(payloadSize int, rttVal float64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()

	// Only update stats if we haven't finished yet to prevent skewing stats after completion
	if f.isFinished {
		return
	}

	f.endTime = now
	f.totalBytes += int64(payloadSize)
	f.pktsReceivedTotal++

	// Check for completion
	if f.pktsReceivedTotal >= MAX_PACKETS {
		f.isFinished = true
		close(f.doneCh)
	}

	// 1. Update Sliding Window (Add new, remove old > 100ms)
	f.window = append(f.window, PacketSample{
		Timestamp: now,
		Size:      payloadSize,
		RTT:       rttVal,
	})

	cutoff := now.Add(-THROUGHPUT_WINDOW_DUR)
	validIdx := 0
	for i, sample := range f.window {
		if sample.Timestamp.After(cutoff) {
			validIdx = i
			break
		}
	}
	// Prune the slice
	f.window = f.window[validIdx:]

	// 2. Initial Adjustment Period Calculation (First 5 packets)
	if f.pktsCalibrated < 5 {
		f.initialRTTSum += rttVal
		f.pktsCalibrated++

		if f.pktsCalibrated == 5 {
			// Calculate Base RTT
			f.baseRTT = f.initialRTTSum / 5.0

			// Calculate adjustment period based on sum of first 5 RTTs
			f.adjustmentPeriod = time.Duration(f.initialRTTSum) * time.Millisecond

			// Safety lower bound for adjustment period
			if f.adjustmentPeriod < 20*time.Millisecond {
				f.adjustmentPeriod = 20 * time.Millisecond
			}

			f.lastRateUpdate = now
			log.Info(consumerTag, "Flow Calibration Complete",
				"flow", f.Prefix,
				"baseRTT", f.baseRTT,
				"period", f.adjustmentPeriod)
		}
		return // Don't adjust rate yet
	}

	// 3. Dynamic Rate Adjustment
	if now.Sub(f.lastRateUpdate) >= f.adjustmentPeriod {

		// Calculate Window Statistics
		var totalRTT float64
		count := len(f.window)

		if count == 0 {
			return
		}

		for _, s := range f.window {
			totalRTT += s.RTT
		}
		windowAvgRTT := totalRTT / float64(count)

		// Calculate Throughput (PPS)
		windowSeconds := THROUGHPUT_WINDOW_DUR.Seconds()
		measuredThroughputPPS := float64(count) / windowSeconds

		// Determine Target Rate based on RTT comparison
		targetRate := f.currentRate

		if windowAvgRTT < 1.5*f.baseRTT {
			// Low congestion: Probe more bandwidth
			targetRate = f.currentRate * 1.1
		} else if windowAvgRTT >= 1.5*f.baseRTT && windowAvgRTT < 2.0*f.baseRTT {
			// Moderate congestion: Hold rate
			targetRate = f.currentRate
		} else {
			// High congestion (>= 2 * base): Back off
			targetRate = f.currentRate * 0.8
		}

		// Apply Throughput Caps
		lowerBound := 0.8 * measuredThroughputPPS
		upperBound := 1.5 * measuredThroughputPPS

		if lowerBound < 1.0 {
			lowerBound = 1.0
		}
		if upperBound < INIT_INTEREST_RATE {
			upperBound = INIT_INTEREST_RATE
		}

		finalRate := targetRate

		if finalRate < lowerBound {
			finalRate = lowerBound
		}
		if finalRate > upperBound {
			finalRate = upperBound
		}

		if finalRate < 10.0 {
			finalRate = 10.0
		}

		prevRate := f.currentRate
		f.currentRate = finalRate
		f.lastRateUpdate = now

		log.Info(consumerTag, "Rate Adjusted",
			"flow", f.Prefix,
			"baseRTT", f.baseRTT,
			"winAvgRTT", windowAvgRTT,
			"measuredPPS", measuredThroughputPPS,
			"oldRate", prevRate,
			"finalRate", f.currentRate)
	}
}

func (f *FlowContext) GetFinalStats() (float64, int64, float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	duration := f.endTime.Sub(f.startTime).Seconds()
	if duration <= 0 {
		return 0.0, f.totalBytes, 0.0
	}
	// Change to Mbps: (Bytes * 8) / (Seconds * 1,000,000)
	throughputMbps := (float64(f.totalBytes) * 8.0) / (duration * 1000000.0)
	return throughputMbps, f.totalBytes, duration
}

// RTTSample holds a single RTT measurement
type RTTSample struct {
	Timestamp time.Time
	RTT       float64 // in milliseconds
}

// RTTTracker structure
type RTTTracker struct {
	mu               sync.RWMutex
	pendingInterests map[string]time.Time // interest name -> send time

	// Per-prefix history for RTO calculation
	rttHistory map[string][]RTTSample

	// Current calculated metrics per prefix
	srtt   map[string]float64 // Stores the Mean RTT (Equation 1)
	rttVar map[string]float64 // Stores the Variance (Equation 2)
	rto    map[string]float64 // Stores the Final Threshold (Equation 4)

	totalSamples int
	totalRTT     float64
}

func NewRTTTracker() *RTTTracker {
	return &RTTTracker{
		pendingInterests: make(map[string]time.Time),
		rttHistory:       make(map[string][]RTTSample),
		srtt:             make(map[string]float64),
		rttVar:           make(map[string]float64),
		rto:              make(map[string]float64),
		totalSamples:     0,
		totalRTT:         0,
	}
}

// GetRTO returns the current RTO for a prefix, or a default value if unknown
func (rtt *RTTTracker) GetRTO(prefix string) float64 {
	rtt.mu.RLock()
	defer rtt.mu.RUnlock()
	if val, ok := rtt.rto[prefix]; ok {
		return val
	}
	return MIN_RTO * 2 // Default conservative RTO
}

func (rtt *RTTTracker) RecordInterestSent(interestName string, prefix string, sendTime time.Time) {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()
	// Overwrite any existing timestamp.
	// This resets the RTT timer for this specific interest name (Sequence Number).
	rtt.pendingInterests[interestName] = sendTime

	if _, exists := rtt.rttHistory[prefix]; !exists {
		rtt.rttHistory[prefix] = make([]RTTSample, 0)
		rtt.srtt[prefix] = 0
		rtt.rttVar[prefix] = 0
		rtt.rto[prefix] = MIN_RTO * 2
	}
}

// RecordDataReceived calculates RTO based on the provided 4 equations.
func (rtt *RTTTracker) RecordDataReceived(dataName string, prefix string, receiveTime time.Time) (float64, float64, float64, float64) {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()

	sendTime, exists := rtt.pendingInterests[dataName]
	if !exists {
		return 0, 0, 0, 0
	}

	rttSampleVal := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

	rtt.totalSamples++
	rtt.totalRTT += rttSampleVal
	delete(rtt.pendingInterests, dataName)

	// Update History Window
	newSample := RTTSample{Timestamp: receiveTime, RTT: rttSampleVal}
	rtt.rttHistory[prefix] = append(rtt.rttHistory[prefix], newSample)

	// Remove samples older than window duration
	history := rtt.rttHistory[prefix]
	validIdx := 0
	for i, sample := range history {
		if receiveTime.Sub(sample.Timestamp) <= RTT_WINDOW_DURATION {
			validIdx = i
			break
		}
	}
	rtt.rttHistory[prefix] = history[validIdx:]
	history = rtt.rttHistory[prefix]

	// --- RTO Calculation (Equations 1, 2, 3, 4) ---

	var meanRTT, variance, calculatedRTO, finalRTO float64

	if len(history) < 2 {
		// Not enough samples for variance.
		meanRTT = rttSampleVal
		variance = 0.0
		calculatedRTO = math.Max(MIN_RTO, 2*rttSampleVal)
		finalRTO = 2 * calculatedRTO
	} else {
		// Equation 1: Sample Mean RTT
		var sum float64
		for _, s := range history {
			sum += s.RTT
		}
		meanRTT = sum / float64(len(history))

		// Equation 2: Sample Variance
		var varSum float64
		for _, s := range history {
			diff := s.RTT - meanRTT
			varSum += diff * diff
		}
		variance = varSum / float64(len(history))

		// Equation 3: RTO = Mean + 4 * sqrt(Variance)
		stdDev := math.Sqrt(variance)
		calculatedRTO = meanRTT + 4*stdDev

		// Equation 4: RTO_threshold = 2 * RTO_final
		finalRTO = 2 * calculatedRTO
	}

	// Apply Constraints to Final RTO
	if finalRTO < MIN_RTO {
		finalRTO = MIN_RTO
	}
	if finalRTO > MAX_RTO {
		finalRTO = MAX_RTO
	}

	// Update State
	rtt.srtt[prefix] = meanRTT
	rtt.rttVar[prefix] = variance
	rtt.rto[prefix] = finalRTO

	return rttSampleVal, meanRTT, variance, finalRTO
}

func (rtt *RTTTracker) RecordInterestTimeout(interestName string) {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()
	delete(rtt.pendingInterests, interestName)
}

func (rtt *RTTTracker) PrintStats() {
	rtt.mu.RLock()
	defer rtt.mu.RUnlock()

	var avgRTT float64
	if rtt.totalSamples > 0 {
		avgRTT = rtt.totalRTT / float64(rtt.totalSamples)
	}

	log.Info(consumerTag, "=== RTT Statistics ===")
	log.Info(consumerTag, "Global RTT Stats", "avgRTT", avgRTT, "samples", rtt.totalSamples)

	for prefix, val := range rtt.srtt {
		log.Info(consumerTag, "Per-Prefix RTO State",
			"prefix", prefix,
			"MeanRTT(SRTT)", val,
			"Variance(RTTVAR)", rtt.rttVar[prefix],
			"FinalThreshold(RTO)", rtt.rto[prefix])
	}
}

type Statistics struct {
	mu              sync.Mutex
	totalSent       int
	dataReceived    int
	nackReceived    int
	timeoutReceived int
	cancelReceived  int
	retransmissions int
}

func (s *Statistics) IncrementSent()    { s.mu.Lock(); defer s.mu.Unlock(); s.totalSent++ }
func (s *Statistics) IncrementData()    { s.mu.Lock(); defer s.mu.Unlock(); s.dataReceived++ }
func (s *Statistics) IncrementNack()    { s.mu.Lock(); defer s.mu.Unlock(); s.nackReceived++ }
func (s *Statistics) IncrementTimeout() { s.mu.Lock(); defer s.mu.Unlock(); s.timeoutReceived++ }
func (s *Statistics) IncrementCancel()  { s.mu.Lock(); defer s.mu.Unlock(); s.cancelReceived++ }
func (s *Statistics) IncrementRetx()    { s.mu.Lock(); defer s.mu.Unlock(); s.retransmissions++ }

func (s *Statistics) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Info(consumerTag, "=== Final Statistics ===")
	log.Info(consumerTag, "Summary",
		"totalSent", s.totalSent,
		"dataReceived", s.dataReceived,
		"nackReceived", s.nackReceived,
		"timeouts", s.timeoutReceived,
		"retransmissions", s.retransmissions)
}

var globalRTTTracker = NewRTTTracker()

// --- Retransmission Logic ---

type PendingPacket struct {
	Name        string
	Prefix      string
	SendTime    time.Time
	SequenceNum int
	FlowCtx     *FlowContext
}

type RetransmissionController struct {
	mu               sync.Mutex
	pendingInterests map[string]PendingPacket // Key: Interest Name
	app              ndn.Engine
	stats            *Statistics
	stopCh           chan struct{}
}

func NewRetransmissionController(app ndn.Engine, stats *Statistics) *RetransmissionController {
	return &RetransmissionController{
		pendingInterests: make(map[string]PendingPacket),
		app:              app,
		stats:            stats,
		stopCh:           make(chan struct{}),
	}
}

func (rc *RetransmissionController) Add(name string, prefix string, seq int, flowCtx *FlowContext) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.pendingInterests[name] = PendingPacket{
		Name:        name,
		Prefix:      prefix,
		SendTime:    time.Now(),
		SequenceNum: seq,
		FlowCtx:     flowCtx,
	}
}

func (rc *RetransmissionController) Remove(name string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	delete(rc.pendingInterests, name)
}

func (rc *RetransmissionController) StartScheduler() {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-rc.stopCh:
			return
		case <-ticker.C:
			rc.checkTimeouts()
		}
	}
}

func (rc *RetransmissionController) Stop() {
	close(rc.stopCh)
}

func (rc *RetransmissionController) checkTimeouts() {
	rc.mu.Lock()
	// We copy pending packets to a slice to avoid holding lock during retransmission
	// However, for simplicity and thread-safety of the map, we can iterate and collect
	// packets to retransmit, then retransmit them.
	var toRetransmit []PendingPacket
	now := time.Now()

	for _, pkt := range rc.pendingInterests {
		// Get Dynamic RTO for this flow
		rtoVal := globalRTTTracker.GetRTO(pkt.Prefix)
		rtoDuration := time.Duration(rtoVal) * time.Millisecond

		if now.Sub(pkt.SendTime) > rtoDuration {
			toRetransmit = append(toRetransmit, pkt)
		}
	}
	rc.mu.Unlock()

	// Perform Retransmissions
	for _, pkt := range toRetransmit {
		// Update the timestamp in the map immediately to prevent double retransmission
		// in the next tick if the network is very slow
		rc.mu.Lock()
		if _, exists := rc.pendingInterests[pkt.Name]; exists {
			// Update send time
			p := rc.pendingInterests[pkt.Name]
			p.SendTime = time.Now()
			rc.pendingInterests[pkt.Name] = p
		} else {
			// Packet might have been received/removed in the meantime
			rc.mu.Unlock()
			continue
		}
		rc.mu.Unlock()

		rc.stats.IncrementRetx()
		log.Warn(consumerTag, "Retransmitting", "name", pkt.Name, "flow", pkt.Prefix)

		// Send the packet again (isRetransmission = true)
		go sendSingleInterest(rc.app, rc.stats, pkt.SequenceNum, pkt.FlowCtx, true)
	}
}

var globalRetxController *RetransmissionController

// --- Helpers ---

func extractPrefix(name enc.Name) string {
	if len(name) > 1 {
		return name.Prefix(len(name) - 1).String()
	}
	return name.String()
}

func parseProducerRange(rangeStr string) ([]string, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format")
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("invalid numbers")
	}
	var prefixes []string
	for i := start; i <= end; i++ {
		prefixes = append(prefixes, fmt.Sprintf("/pro%dapp", i))
	}
	return prefixes, nil
}

func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int, flowCtx *FlowContext, isRetransmission bool) {
	name, err := enc.NameFromStr(flowCtx.Prefix)
	if err != nil {
		return
	}

	seqStr := fmt.Sprintf("seq-%d", sequenceNum)
	name = name.Append(enc.NewStringComponent(0x08, seqStr))

	intCfg := &ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(INTEREST_LIFETIME),
		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
	}

	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
	if err != nil {
		return
	}

	stats.IncrementSent()
	interestName := interest.FinalName.String()
	sendTime := time.Now()

	// 1. Record for RTT calculation
	// MODIFIED: We record this for EVERY send attempt (including retransmissions).
	// This resets the timer for this specific interest, so if we receive Data,
	// the RTT will reflect the time since the *latest* retransmission.
	globalRTTTracker.RecordInterestSent(interestName, flowCtx.Prefix, sendTime)

	// 2. Register for Retransmission Check (Update timestamp if it exists, add if new)
	globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx)

	err = app.Express(interest,
		func(args ndn.ExpressCallbackArgs) {
			receiveTime := time.Now()
			switch args.Result {
			case ndn.InterestResultNack:
				stats.IncrementNack()
				globalRTTTracker.RecordInterestTimeout(interestName)
				globalRetxController.Remove(interestName) // Stop retransmitting
			case ndn.InterestResultTimeout:
				stats.IncrementTimeout()
				globalRTTTracker.RecordInterestTimeout(interestName)
				globalRetxController.Remove(interestName) // Engine gave up, we stop too
			case ndn.InterestCancelled:
				stats.IncrementCancel()
				globalRTTTracker.RecordInterestTimeout(interestName)
				globalRetxController.Remove(interestName)
			case ndn.InterestResultData:
				data := args.Data
				content := data.Content().Join()
				dataName := data.Name().String()
				prefix := extractPrefix(data.Name())

				// Stop Retransmission Tracking
				globalRetxController.Remove(dataName)

				// 1. RTO Calculation
				rttSample, mean, variance, rto := globalRTTTracker.RecordDataReceived(dataName, prefix, receiveTime)

				// 2. Throughput & Rate Control Update
				flowCtx.RecordPacket(len(content), rttSample)

				log.Info(consumerTag, "Data Received",
					"seq", sequenceNum,
					"prefix", prefix,
					"rtt", rttSample,
					"MeanRTT", mean,
					"Variance", variance,
					"FinalRTO", rto,
					"currentRate", flowCtx.GetRate())

				stats.IncrementData()
			}
		})
}

func runFlow(app ndn.Engine, stats *Statistics, flowCtx *FlowContext, wg *sync.WaitGroup) {
	defer wg.Done()

	totalTimer := time.NewTimer(TOTAL_DURATION)
	defer totalTimer.Stop()

	sequenceNum := 0

	log.Info(consumerTag, "Starting flow",
		"target", flowCtx.Prefix,
		"initRate", INIT_INTEREST_RATE,
		"maxPackets", MAX_PACKETS)

	for {
		select {
		case <-totalTimer.C:
			log.Info(consumerTag, "Flow stopped (Timeout)", "target", flowCtx.Prefix)
			return
		case <-flowCtx.doneCh:
			log.Info(consumerTag, "Flow stopped (Completed)", "target", flowCtx.Prefix)
			return
		default:
			// Check if we have sent enough interests
			if sequenceNum >= MAX_PACKETS {
				// We have sent the max number of interests.
				// Do NOT return yet. We must wait for the packets to be received (via doneCh)
				// or for the hard timeout (via totalTimer).
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Dynamic Rate Control
			currentRate := flowCtx.GetRate()

			if currentRate <= 0 {
				currentRate = 1
			}

			sleepDuration := time.Duration(float64(time.Second) / currentRate)
			time.Sleep(sleepDuration)

			sequenceNum++
			// Send new packet (isRetransmission = false)
			go sendSingleInterest(app, stats, sequenceNum, flowCtx, false)
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node_name> <producer_range>\n", os.Args[0])
		os.Exit(1)
	}

	nodeName := os.Args[1]
	rangeStr := os.Args[2]

	targetPrefixes, err := parseProducerRange(rangeStr)
	if err != nil {
		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
		return
	}

	log.Default().SetLevel(log.LevelInfo)
	os.Setenv("NDN_CLIENT_TRANSPORT", "tcp://127.0.0.1:6363")

	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err = app.Start()
	if err != nil {
		log.Fatal(consumerTag, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	stats := &Statistics{}

	// Initialize Retransmission Controller
	globalRetxController = NewRetransmissionController(app, stats)
	go globalRetxController.StartScheduler()
	defer globalRetxController.Stop()

	log.Info(consumerTag, "Starting NDN Consumer",
		"nodeName", nodeName,
		"targets", targetPrefixes,
		"initRate", INIT_INTEREST_RATE,
		"maxPackets", MAX_PACKETS,
		"totalDuration", TOTAL_DURATION)

	var wg sync.WaitGroup
	startTime := time.Now()
	flowContexts := make([]*FlowContext, 0, len(targetPrefixes))

	for _, prefix := range targetPrefixes {
		wg.Add(1)
		flowCtx := NewFlowContext(prefix)
		flowContexts = append(flowContexts, flowCtx)
		go runFlow(app, stats, flowCtx, &wg)
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	log.Info(consumerTag, "All flows stopped", "elapsedTime", elapsed)

	// Wait a bit for any lingering logs/stats updates
	time.Sleep(1 * time.Second)

	stats.Print()
	globalRTTTracker.PrintStats()

	log.Info(consumerTag, "=== Final Per-Flow Statistics ===")
	for _, flowCtx := range flowContexts {
		avgThroughput, totalBytes, duration := flowCtx.GetFinalStats()
		log.Info(consumerTag, "Flow Summary",
			"flow", flowCtx.Prefix,
			"totalBytes", totalBytes,
			"timeTakenSec", duration,
			"avgThroughput", avgThroughput,
			"unit", "Mbps")
	}
}

// package main

// import (
// 	"fmt"
// 	"math"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	enc "github.com/named-data/ndnd/std/encoding"
// 	"github.com/named-data/ndnd/std/engine"
// 	"github.com/named-data/ndnd/std/log"
// 	"github.com/named-data/ndnd/std/ndn"
// 	"github.com/named-data/ndnd/std/types/optional"
// 	"github.com/named-data/ndnd/std/utils"
// )

// const (
// 	INIT_INTEREST_RATE    = 2000.0                 // Initial Rate: 2000 interests/sec
// 	TOTAL_DURATION        = 10 * time.Second       // Max duration (hard stop)
// 	MAX_PACKETS           = 30000                  // Target packets to receive per flow
// 	INTEREST_LIFETIME     = 4 * time.Second        // Individual Interest lifetime
// 	RTT_WINDOW_DURATION   = 1 * time.Second        // Duration to keep RTT samples for RTO calc
// 	MIN_RTO               = 30.0                   // Minimum RTO in milliseconds
// 	THROUGHPUT_WINDOW_DUR = 100 * time.Millisecond // Sliding window size for throughput/RTT
// 	CHECK_INTERVAL        = 30 * time.Millisecond  // Retransmission check interval
// )

// // Create a tag for our consumer application
// type ConsumerTag struct{}

// func (ConsumerTag) String() string {
// 	return "NDN-Consumer"
// }

// var consumerTag = ConsumerTag{}

// // PacketSample helps track throughput and RTT in the sliding window
// type PacketSample struct {
// 	Timestamp time.Time
// 	Size      int
// 	RTT       float64 // in milliseconds
// }

// // FlowContext holds state specific to a single flow (target prefix)
// type FlowContext struct {
// 	mu     sync.Mutex
// 	Prefix string

// 	// Statistics
// 	totalBytes        int64
// 	pktsReceivedTotal int // Total valid Data packets received
// 	startTime         time.Time
// 	endTime           time.Time
// 	isFinished        bool
// 	doneCh            chan struct{} // Signal channel when MAX_PACKETS reached

// 	// Unified Sliding Window (100ms)
// 	window []PacketSample

// 	// Rate Control State
// 	currentRate      float64       // Interests per second
// 	pktsCalibrated   int           // Count for initial calibration (distinct from total)
// 	initialRTTSum    float64       // Sum of first 5 RTTs
// 	baseRTT          float64       // Average of first 5 RTTs (Baseline)
// 	adjustmentPeriod time.Duration // Calculated from first 5 packets
// 	lastRateUpdate   time.Time     // Last time we adjusted the rate
// }

// func NewFlowContext(prefix string) *FlowContext {
// 	now := time.Now()
// 	return &FlowContext{
// 		Prefix:         prefix,
// 		startTime:      now,
// 		endTime:        now,
// 		window:         make([]PacketSample, 0),
// 		currentRate:    INIT_INTEREST_RATE,
// 		pktsCalibrated: 0,
// 		lastRateUpdate: now,
// 		doneCh:         make(chan struct{}),
// 	}
// }

// // GetRate returns the current dynamic sending rate
// func (f *FlowContext) GetRate() float64 {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()
// 	return f.currentRate
// }

// // RecordPacket updates window, stats, and adjusts sending rate based on RTT congestion control
// func (f *FlowContext) RecordPacket(payloadSize int, rttVal float64) {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()

// 	now := time.Now()

// 	// Only update stats if we haven't finished yet to prevent skewing stats after completion
// 	if f.isFinished {
// 		return
// 	}

// 	f.endTime = now
// 	f.totalBytes += int64(payloadSize)
// 	f.pktsReceivedTotal++

// 	// Check for completion
// 	if f.pktsReceivedTotal >= MAX_PACKETS {
// 		f.isFinished = true
// 		close(f.doneCh)
// 	}

// 	// 1. Update Sliding Window (Add new, remove old > 100ms)
// 	f.window = append(f.window, PacketSample{
// 		Timestamp: now,
// 		Size:      payloadSize,
// 		RTT:       rttVal,
// 	})

// 	cutoff := now.Add(-THROUGHPUT_WINDOW_DUR)
// 	validIdx := 0
// 	for i, sample := range f.window {
// 		if sample.Timestamp.After(cutoff) {
// 			validIdx = i
// 			break
// 		}
// 	}
// 	// Prune the slice
// 	f.window = f.window[validIdx:]

// 	// 2. Initial Adjustment Period Calculation (First 5 packets)
// 	if f.pktsCalibrated < 5 {
// 		f.initialRTTSum += rttVal
// 		f.pktsCalibrated++

// 		if f.pktsCalibrated == 5 {
// 			// Calculate Base RTT
// 			f.baseRTT = f.initialRTTSum / 5.0

// 			// Calculate adjustment period based on sum of first 5 RTTs
// 			f.adjustmentPeriod = time.Duration(f.initialRTTSum) * time.Millisecond

// 			// Safety lower bound for adjustment period
// 			if f.adjustmentPeriod < 20*time.Millisecond {
// 				f.adjustmentPeriod = 20 * time.Millisecond
// 			}

// 			f.lastRateUpdate = now
// 			log.Info(consumerTag, "Flow Calibration Complete",
// 				"flow", f.Prefix,
// 				"baseRTT", f.baseRTT,
// 				"period", f.adjustmentPeriod)
// 		}
// 		return // Don't adjust rate yet
// 	}

// 	// 3. Dynamic Rate Adjustment
// 	if now.Sub(f.lastRateUpdate) >= f.adjustmentPeriod {

// 		// Calculate Window Statistics
// 		var totalRTT float64
// 		count := len(f.window)

// 		if count == 0 {
// 			return
// 		}

// 		for _, s := range f.window {
// 			totalRTT += s.RTT
// 		}
// 		windowAvgRTT := totalRTT / float64(count)

// 		// Calculate Throughput (PPS)
// 		windowSeconds := THROUGHPUT_WINDOW_DUR.Seconds()
// 		measuredThroughputPPS := float64(count) / windowSeconds

// 		// Determine Target Rate based on RTT comparison
// 		targetRate := f.currentRate

// 		if windowAvgRTT < 1.5*f.baseRTT {
// 			// Low congestion: Probe more bandwidth
// 			targetRate = f.currentRate * 1.1
// 		} else if windowAvgRTT >= 1.5*f.baseRTT && windowAvgRTT < 2.0*f.baseRTT {
// 			// Moderate congestion: Hold rate
// 			targetRate = f.currentRate
// 		} else {
// 			// High congestion (>= 2 * base): Back off
// 			targetRate = f.currentRate * 0.8
// 		}

// 		// Apply Throughput Caps
// 		lowerBound := 0.8 * measuredThroughputPPS
// 		upperBound := 1.5 * measuredThroughputPPS

// 		if lowerBound < 1.0 {
// 			lowerBound = 1.0
// 		}
// 		if upperBound < INIT_INTEREST_RATE {
// 			upperBound = INIT_INTEREST_RATE
// 		}

// 		finalRate := targetRate

// 		if finalRate < lowerBound {
// 			finalRate = lowerBound
// 		}
// 		if finalRate > upperBound {
// 			finalRate = upperBound
// 		}

// 		if finalRate < 10.0 {
// 			finalRate = 10.0
// 		}

// 		prevRate := f.currentRate
// 		f.currentRate = finalRate
// 		f.lastRateUpdate = now

// 		log.Info(consumerTag, "Rate Adjusted",
// 			"flow", f.Prefix,
// 			"baseRTT", f.baseRTT,
// 			"winAvgRTT", windowAvgRTT,
// 			"measuredPPS", measuredThroughputPPS,
// 			"oldRate", prevRate,
// 			"finalRate", f.currentRate)
// 	}
// }

// func (f *FlowContext) GetFinalStats() (float64, int64, float64) {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()
// 	duration := f.endTime.Sub(f.startTime).Seconds()
// 	if duration <= 0 {
// 		return 0.0, f.totalBytes, 0.0
// 	}
// 	// Change to Mbps: (Bytes * 8) / (Seconds * 1,000,000)
// 	throughputMbps := (float64(f.totalBytes) * 8.0) / (duration * 1000000.0)
// 	return throughputMbps, f.totalBytes, duration
// }

// // RTTSample holds a single RTT measurement
// type RTTSample struct {
// 	Timestamp time.Time
// 	RTT       float64 // in milliseconds
// }

// // RTTTracker structure
// type RTTTracker struct {
// 	mu               sync.RWMutex
// 	pendingInterests map[string]time.Time // interest name -> send time

// 	// Per-prefix history for RTO calculation
// 	rttHistory map[string][]RTTSample

// 	// Current calculated metrics per prefix
// 	srtt   map[string]float64 // Stores the Mean RTT (Equation 1)
// 	rttVar map[string]float64 // Stores the Variance (Equation 2)
// 	rto    map[string]float64 // Stores the Final Threshold (Equation 4)

// 	totalSamples int
// 	totalRTT     float64
// }

// func NewRTTTracker() *RTTTracker {
// 	return &RTTTracker{
// 		pendingInterests: make(map[string]time.Time),
// 		rttHistory:       make(map[string][]RTTSample),
// 		srtt:             make(map[string]float64),
// 		rttVar:           make(map[string]float64),
// 		rto:              make(map[string]float64),
// 		totalSamples:     0,
// 		totalRTT:         0,
// 	}
// }

// // GetRTO returns the current RTO for a prefix, or a default value if unknown
// func (rtt *RTTTracker) GetRTO(prefix string) float64 {
// 	rtt.mu.RLock()
// 	defer rtt.mu.RUnlock()
// 	if val, ok := rtt.rto[prefix]; ok {
// 		return val
// 	}
// 	return MIN_RTO * 2 // Default conservative RTO
// }

// func (rtt *RTTTracker) RecordInterestSent(interestName string, prefix string, sendTime time.Time) {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()
// 	rtt.pendingInterests[interestName] = sendTime

// 	if _, exists := rtt.rttHistory[prefix]; !exists {
// 		rtt.rttHistory[prefix] = make([]RTTSample, 0)
// 		rtt.srtt[prefix] = 0
// 		rtt.rttVar[prefix] = 0
// 		rtt.rto[prefix] = MIN_RTO * 2
// 	}
// }

// // RecordDataReceived calculates RTO based on the provided 4 equations.
// func (rtt *RTTTracker) RecordDataReceived(dataName string, prefix string, receiveTime time.Time) (float64, float64, float64, float64) {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()

// 	sendTime, exists := rtt.pendingInterests[dataName]
// 	if !exists {
// 		return 0, 0, 0, 0
// 	}

// 	rttSampleVal := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

// 	rtt.totalSamples++
// 	rtt.totalRTT += rttSampleVal
// 	delete(rtt.pendingInterests, dataName)

// 	// Update History Window
// 	newSample := RTTSample{Timestamp: receiveTime, RTT: rttSampleVal}
// 	rtt.rttHistory[prefix] = append(rtt.rttHistory[prefix], newSample)

// 	// Remove samples older than window duration
// 	history := rtt.rttHistory[prefix]
// 	validIdx := 0
// 	for i, sample := range history {
// 		if receiveTime.Sub(sample.Timestamp) <= RTT_WINDOW_DURATION {
// 			validIdx = i
// 			break
// 		}
// 	}
// 	rtt.rttHistory[prefix] = history[validIdx:]
// 	history = rtt.rttHistory[prefix]

// 	// --- RTO Calculation (Equations 1, 2, 3, 4) ---

// 	var meanRTT, variance, calculatedRTO, finalRTO float64

// 	if len(history) < 2 {
// 		// Not enough samples for variance.
// 		meanRTT = rttSampleVal
// 		variance = 0.0
// 		calculatedRTO = math.Max(MIN_RTO, 2*rttSampleVal)
// 		finalRTO = 2 * calculatedRTO
// 	} else {
// 		// Equation 1: Sample Mean RTT
// 		var sum float64
// 		for _, s := range history {
// 			sum += s.RTT
// 		}
// 		meanRTT = sum / float64(len(history))

// 		// Equation 2: Sample Variance
// 		var varSum float64
// 		for _, s := range history {
// 			diff := s.RTT - meanRTT
// 			varSum += diff * diff
// 		}
// 		variance = varSum / float64(len(history))

// 		// Equation 3: RTO = Mean + 4 * sqrt(Variance)
// 		stdDev := math.Sqrt(variance)
// 		calculatedRTO = meanRTT + 4*stdDev

// 		// Clamp RTO
// 		if calculatedRTO < MIN_RTO {
// 			calculatedRTO = MIN_RTO
// 		}

// 		// Equation 4: RTO_threshold = 2 * RTO_final
// 		finalRTO = 2 * calculatedRTO
// 	}

// 	// Update State
// 	rtt.srtt[prefix] = meanRTT
// 	rtt.rttVar[prefix] = variance
// 	rtt.rto[prefix] = finalRTO

// 	return rttSampleVal, meanRTT, variance, finalRTO
// }

// func (rtt *RTTTracker) RecordInterestTimeout(interestName string) {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()
// 	delete(rtt.pendingInterests, interestName)
// }

// func (rtt *RTTTracker) PrintStats() {
// 	rtt.mu.RLock()
// 	defer rtt.mu.RUnlock()

// 	var avgRTT float64
// 	if rtt.totalSamples > 0 {
// 		avgRTT = rtt.totalRTT / float64(rtt.totalSamples)
// 	}

// 	log.Info(consumerTag, "=== RTT Statistics ===")
// 	log.Info(consumerTag, "Global RTT Stats", "avgRTT", avgRTT, "samples", rtt.totalSamples)

// 	for prefix, val := range rtt.srtt {
// 		log.Info(consumerTag, "Per-Prefix RTO State",
// 			"prefix", prefix,
// 			"MeanRTT(SRTT)", val,
// 			"Variance(RTTVAR)", rtt.rttVar[prefix],
// 			"FinalThreshold(RTO)", rtt.rto[prefix])
// 	}
// }

// type Statistics struct {
// 	mu              sync.Mutex
// 	totalSent       int
// 	dataReceived    int
// 	nackReceived    int
// 	timeoutReceived int
// 	cancelReceived  int
// 	retransmissions int
// }

// func (s *Statistics) IncrementSent()    { s.mu.Lock(); defer s.mu.Unlock(); s.totalSent++ }
// func (s *Statistics) IncrementData()    { s.mu.Lock(); defer s.mu.Unlock(); s.dataReceived++ }
// func (s *Statistics) IncrementNack()    { s.mu.Lock(); defer s.mu.Unlock(); s.nackReceived++ }
// func (s *Statistics) IncrementTimeout() { s.mu.Lock(); defer s.mu.Unlock(); s.timeoutReceived++ }
// func (s *Statistics) IncrementCancel()  { s.mu.Lock(); defer s.mu.Unlock(); s.cancelReceived++ }
// func (s *Statistics) IncrementRetx()    { s.mu.Lock(); defer s.mu.Unlock(); s.retransmissions++ }

// func (s *Statistics) Print() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	log.Info(consumerTag, "=== Final Statistics ===")
// 	log.Info(consumerTag, "Summary",
// 		"totalSent", s.totalSent,
// 		"dataReceived", s.dataReceived,
// 		"nackReceived", s.nackReceived,
// 		"timeouts", s.timeoutReceived,
// 		"retransmissions", s.retransmissions)
// }

// var globalRTTTracker = NewRTTTracker()

// // --- Retransmission Logic ---

// type PendingPacket struct {
// 	Name        string
// 	Prefix      string
// 	SendTime    time.Time
// 	SequenceNum int
// 	FlowCtx     *FlowContext
// }

// type RetransmissionController struct {
// 	mu               sync.Mutex
// 	pendingInterests map[string]PendingPacket // Key: Interest Name
// 	app              ndn.Engine
// 	stats            *Statistics
// 	stopCh           chan struct{}
// }

// func NewRetransmissionController(app ndn.Engine, stats *Statistics) *RetransmissionController {
// 	return &RetransmissionController{
// 		pendingInterests: make(map[string]PendingPacket),
// 		app:              app,
// 		stats:            stats,
// 		stopCh:           make(chan struct{}),
// 	}
// }

// func (rc *RetransmissionController) Add(name string, prefix string, seq int, flowCtx *FlowContext) {
// 	rc.mu.Lock()
// 	defer rc.mu.Unlock()
// 	rc.pendingInterests[name] = PendingPacket{
// 		Name:        name,
// 		Prefix:      prefix,
// 		SendTime:    time.Now(),
// 		SequenceNum: seq,
// 		FlowCtx:     flowCtx,
// 	}
// }

// func (rc *RetransmissionController) Remove(name string) {
// 	rc.mu.Lock()
// 	defer rc.mu.Unlock()
// 	delete(rc.pendingInterests, name)
// }

// func (rc *RetransmissionController) StartScheduler() {
// 	ticker := time.NewTicker(CHECK_INTERVAL)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-rc.stopCh:
// 			return
// 		case <-ticker.C:
// 			rc.checkTimeouts()
// 		}
// 	}
// }

// func (rc *RetransmissionController) Stop() {
// 	close(rc.stopCh)
// }

// func (rc *RetransmissionController) checkTimeouts() {
// 	rc.mu.Lock()
// 	// We copy pending packets to a slice to avoid holding lock during retransmission
// 	// However, for simplicity and thread-safety of the map, we can iterate and collect
// 	// packets to retransmit, then retransmit them.
// 	var toRetransmit []PendingPacket
// 	now := time.Now()

// 	for _, pkt := range rc.pendingInterests {
// 		// Get Dynamic RTO for this flow
// 		rtoVal := globalRTTTracker.GetRTO(pkt.Prefix)
// 		rtoDuration := time.Duration(rtoVal) * time.Millisecond

// 		if now.Sub(pkt.SendTime) > rtoDuration {
// 			toRetransmit = append(toRetransmit, pkt)
// 		}
// 	}
// 	rc.mu.Unlock()

// 	// Perform Retransmissions
// 	for _, pkt := range toRetransmit {
// 		// Update the timestamp in the map immediately to prevent double retransmission
// 		// in the next tick if the network is very slow
// 		rc.mu.Lock()
// 		if _, exists := rc.pendingInterests[pkt.Name]; exists {
// 			// Update send time
// 			p := rc.pendingInterests[pkt.Name]
// 			p.SendTime = time.Now()
// 			rc.pendingInterests[pkt.Name] = p
// 		} else {
// 			// Packet might have been received/removed in the meantime
// 			rc.mu.Unlock()
// 			continue
// 		}
// 		rc.mu.Unlock()

// 		rc.stats.IncrementRetx()
// 		log.Warn(consumerTag, "Retransmitting", "name", pkt.Name, "flow", pkt.Prefix)

// 		// Send the packet again (isRetransmission = true)
// 		go sendSingleInterest(rc.app, rc.stats, pkt.SequenceNum, pkt.FlowCtx, true)
// 	}
// }

// var globalRetxController *RetransmissionController

// // --- Helpers ---

// func extractPrefix(name enc.Name) string {
// 	if len(name) > 1 {
// 		return name.Prefix(len(name) - 1).String()
// 	}
// 	return name.String()
// }

// func parseProducerRange(rangeStr string) ([]string, error) {
// 	parts := strings.Split(rangeStr, "-")
// 	if len(parts) != 2 {
// 		return nil, fmt.Errorf("invalid format")
// 	}
// 	start, err1 := strconv.Atoi(parts[0])
// 	end, err2 := strconv.Atoi(parts[1])
// 	if err1 != nil || err2 != nil {
// 		return nil, fmt.Errorf("invalid numbers")
// 	}
// 	var prefixes []string
// 	for i := start; i <= end; i++ {
// 		prefixes = append(prefixes, fmt.Sprintf("/pro%dapp", i))
// 	}
// 	return prefixes, nil
// }

// func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int, flowCtx *FlowContext, isRetransmission bool) {
// 	name, err := enc.NameFromStr(flowCtx.Prefix)
// 	if err != nil {
// 		return
// 	}

// 	seqStr := fmt.Sprintf("seq-%d", sequenceNum)
// 	name = name.Append(enc.NewStringComponent(0x08, seqStr))

// 	intCfg := &ndn.InterestConfig{
// 		MustBeFresh: true,
// 		Lifetime:    optional.Some(INTEREST_LIFETIME),
// 		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
// 	}

// 	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
// 	if err != nil {
// 		return
// 	}

// 	stats.IncrementSent()
// 	interestName := interest.FinalName.String()
// 	sendTime := time.Now()

// 	// 1. Record for RTT calculation (only if NOT a retransmission, to avoid RTT ambiguity)
// 	//    Standard TCP RTT sampling usually ignores retransmissions (Karn's Algorithm).
// 	if !isRetransmission {
// 		globalRTTTracker.RecordInterestSent(interestName, flowCtx.Prefix, sendTime)
// 	}

// 	// 2. Register for Retransmission Check (Update timestamp if it exists, add if new)
// 	globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx)

// 	err = app.Express(interest,
// 		func(args ndn.ExpressCallbackArgs) {
// 			receiveTime := time.Now()
// 			switch args.Result {
// 			case ndn.InterestResultNack:
// 				stats.IncrementNack()
// 				globalRTTTracker.RecordInterestTimeout(interestName)
// 				globalRetxController.Remove(interestName) // Stop retransmitting
// 			case ndn.InterestResultTimeout:
// 				stats.IncrementTimeout()
// 				globalRTTTracker.RecordInterestTimeout(interestName)
// 				globalRetxController.Remove(interestName) // Engine gave up, we stop too
// 			case ndn.InterestCancelled:
// 				stats.IncrementCancel()
// 				globalRTTTracker.RecordInterestTimeout(interestName)
// 				globalRetxController.Remove(interestName)
// 			case ndn.InterestResultData:
// 				data := args.Data
// 				content := data.Content().Join()
// 				dataName := data.Name().String()
// 				prefix := extractPrefix(data.Name())

// 				// Stop Retransmission Tracking
// 				globalRetxController.Remove(dataName)

// 				// 1. RTO Calculation (Karn's algorithm implies we might skip this if it was retransmitted,
// 				//    but our RTTTracker handles 'pendingInterests' logic where it deletes entry on first read.
// 				//    If this was a retransmission, RecordDataReceived might return 0 if original send time was lost/overwritten.
// 				//    However, for simplicity here, we just feed it.)
// 				rttSample, mean, variance, rto := globalRTTTracker.RecordDataReceived(dataName, prefix, receiveTime)

// 				// 2. Throughput & Rate Control Update
// 				flowCtx.RecordPacket(len(content), rttSample)

// 				log.Info(consumerTag, "Data Received",
// 					"seq", sequenceNum,
// 					"prefix", prefix,
// 					"rtt", rttSample,
// 					"MeanRTT", mean,
// 					"Variance", variance,
// 					"FinalRTO", rto,
// 					"currentRate", flowCtx.GetRate())

// 				stats.IncrementData()
// 			}
// 		})
// }

// func runFlow(app ndn.Engine, stats *Statistics, flowCtx *FlowContext, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	totalTimer := time.NewTimer(TOTAL_DURATION)
// 	defer totalTimer.Stop()

// 	sequenceNum := 0

// 	log.Info(consumerTag, "Starting flow",
// 		"target", flowCtx.Prefix,
// 		"initRate", INIT_INTEREST_RATE,
// 		"maxPackets", MAX_PACKETS)

// 	for {
// 		select {
// 		case <-totalTimer.C:
// 			log.Info(consumerTag, "Flow stopped (Timeout)", "target", flowCtx.Prefix)
// 			return
// 		case <-flowCtx.doneCh:
// 			log.Info(consumerTag, "Flow stopped (Completed)", "target", flowCtx.Prefix)
// 			return
// 		default:
// 			// Check if we have sent enough interests
// 			if sequenceNum >= MAX_PACKETS {
// 				// We have sent the max number of interests.
// 				// Do NOT return yet. We must wait for the packets to be received (via doneCh)
// 				// or for the hard timeout (via totalTimer).
// 				time.Sleep(10 * time.Millisecond)
// 				continue
// 			}

// 			// Dynamic Rate Control
// 			currentRate := flowCtx.GetRate()

// 			if currentRate <= 0 {
// 				currentRate = 1
// 			}

// 			sleepDuration := time.Duration(float64(time.Second) / currentRate)
// 			time.Sleep(sleepDuration)

// 			sequenceNum++
// 			// Send new packet (isRetransmission = false)
// 			go sendSingleInterest(app, stats, sequenceNum, flowCtx, false)
// 		}
// 	}
// }

// func main() {
// 	if len(os.Args) < 3 {
// 		fmt.Fprintf(os.Stderr, "Usage: %s <node_name> <producer_range>\n", os.Args[0])
// 		os.Exit(1)
// 	}

// 	nodeName := os.Args[1]
// 	rangeStr := os.Args[2]

// 	targetPrefixes, err := parseProducerRange(rangeStr)
// 	if err != nil {
// 		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
// 		return
// 	}

// 	log.Default().SetLevel(log.LevelInfo)
// 	os.Setenv("NDN_CLIENT_TRANSPORT", "tcp://127.0.0.1:6363")

// 	app := engine.NewBasicEngine(engine.NewDefaultFace())
// 	err = app.Start()
// 	if err != nil {
// 		log.Fatal(consumerTag, "Unable to start engine", "err", err)
// 		return
// 	}
// 	defer app.Stop()

// 	stats := &Statistics{}

// 	// Initialize Retransmission Controller
// 	globalRetxController = NewRetransmissionController(app, stats)
// 	go globalRetxController.StartScheduler()
// 	defer globalRetxController.Stop()

// 	log.Info(consumerTag, "Starting NDN Consumer",
// 		"nodeName", nodeName,
// 		"targets", targetPrefixes,
// 		"initRate", INIT_INTEREST_RATE,
// 		"maxPackets", MAX_PACKETS,
// 		"totalDuration", TOTAL_DURATION)

// 	var wg sync.WaitGroup
// 	startTime := time.Now()
// 	flowContexts := make([]*FlowContext, 0, len(targetPrefixes))

// 	for _, prefix := range targetPrefixes {
// 		wg.Add(1)
// 		flowCtx := NewFlowContext(prefix)
// 		flowContexts = append(flowContexts, flowCtx)
// 		go runFlow(app, stats, flowCtx, &wg)
// 	}

// 	wg.Wait()
// 	elapsed := time.Since(startTime)
// 	log.Info(consumerTag, "All flows stopped", "elapsedTime", elapsed)

// 	// Wait a bit for any lingering logs/stats updates
// 	time.Sleep(1 * time.Second)

// 	stats.Print()
// 	globalRTTTracker.PrintStats()

// 	log.Info(consumerTag, "=== Final Per-Flow Statistics ===")
// 	for _, flowCtx := range flowContexts {
// 		avgThroughput, totalBytes, duration := flowCtx.GetFinalStats()
// 		log.Info(consumerTag, "Flow Summary",
// 			"flow", flowCtx.Prefix,
// 			"totalBytes", totalBytes,
// 			"timeTakenSec", duration,
// 			"avgThroughput", avgThroughput,
// 			"unit", "Mbps")
// 	}
// }
