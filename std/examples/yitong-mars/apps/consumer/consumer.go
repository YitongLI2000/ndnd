package main

import (
	"fmt"
	"ndnd-mars/internal/datapacket"
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
	INTERESTS_PER_SECOND = 10               // Rate: 10 interests/sec
	TOTAL_DURATION       = 10 * time.Second // Total duration: 10 seconds
	INTEREST_LIFETIME    = 6 * time.Second  // Individual Interest lifetime
	EWMA_FACTOR          = 0.1              // EWMA smoothing factor
)

// Create a tag for our consumer application
type ConsumerTag struct{}

func (ConsumerTag) String() string {
	return "NDN-Consumer"
}

var consumerTag = ConsumerTag{}

// RTT tracking structure
type RTTTracker struct {
	mu               sync.RWMutex
	estimatedRTT     map[string]float64   // prefix -> estimated RTT in milliseconds
	pendingInterests map[string]time.Time // interest name -> send time
	ewmaFactor       float64
	totalSamples     int
	totalRTT         float64
}

func NewRTTTracker() *RTTTracker {
	return &RTTTracker{
		estimatedRTT:     make(map[string]float64),
		pendingInterests: make(map[string]time.Time),
		ewmaFactor:       EWMA_FACTOR,
		totalSamples:     0,
		totalRTT:         0,
	}
}

func (rtt *RTTTracker) RecordInterestSent(interestName string, prefix string, sendTime time.Time) {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()

	rtt.pendingInterests[interestName] = sendTime

	// Initialize RTT estimation for new prefix if not exists
	if _, exists := rtt.estimatedRTT[prefix]; !exists {
		rtt.estimatedRTT[prefix] = 10.0 // Initialize with 10 milliseconds default
		log.Debug(consumerTag, "Initialized RTT estimation for prefix",
			"prefix", prefix, "initialRTT", 10.0)
	}
}

func (rtt *RTTTracker) RecordDataReceived(dataName string, prefix string, receiveTime time.Time) float64 {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()

	sendTime, exists := rtt.pendingInterests[dataName]
	if !exists {
		log.Warn(consumerTag, "No pending interest found for name", "name", dataName)
		return 0
	}

	// Calculate RTT in milliseconds
	rttSample := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

	// Update EWMA: RTT_estimation = EWMA_FACTOR * RTT_estimation + (1 - EWMA_FACTOR) * sample
	currentEstimation, exists := rtt.estimatedRTT[prefix]
	if !exists {
		currentEstimation = 10.0 // fallback default
	}

	newEstimation := rtt.ewmaFactor*currentEstimation + (1-rtt.ewmaFactor)*rttSample
	rtt.estimatedRTT[prefix] = newEstimation

	// Update global statistics
	rtt.totalSamples++
	rtt.totalRTT += rttSample

	// Clean up pending interest
	delete(rtt.pendingInterests, dataName)

	log.Debug(consumerTag, "RTT Updated",
		"interestName", dataName,
		"sampleRTT", rttSample,
		"newRttEstimation", newEstimation)

	return rttSample
}

func (rtt *RTTTracker) RecordInterestTimeout(interestName string) {
	rtt.mu.Lock()
	defer rtt.mu.Unlock()

	// Clean up pending interest on timeout
	delete(rtt.pendingInterests, interestName)
}

func (rtt *RTTTracker) GetEstimatedRTT(prefix string) float64 {
	rtt.mu.RLock()
	defer rtt.mu.RUnlock()

	estimation, exists := rtt.estimatedRTT[prefix]
	if !exists {
		return 10.0 // Default 10 milliseconds
	}
	return estimation
}

func (rtt *RTTTracker) GetGlobalStats() (avgRTT float64, samples int, estimations map[string]float64) {
	rtt.mu.RLock()
	defer rtt.mu.RUnlock()

	if rtt.totalSamples > 0 {
		avgRTT = rtt.totalRTT / float64(rtt.totalSamples)
	}

	// Make a copy of estimations map
	estimations = make(map[string]float64)
	for prefix, est := range rtt.estimatedRTT {
		estimations[prefix] = est
	}

	return avgRTT, rtt.totalSamples, estimations
}

func (rtt *RTTTracker) PrintStats() {
	avgRTT, samples, estimations := rtt.GetGlobalStats()

	log.Info(consumerTag, "=== RTT Statistics ===")
	log.Info(consumerTag, "Global RTT Stats",
		"avgRTT", avgRTT,
		"samples", samples,
		"unit", "ms")

	for prefix, estimation := range estimations {
		log.Info(consumerTag, "Per-Prefix RTT Estimation",
			"prefix", prefix,
			"estimatedRTT", estimation,
			"unit", "ms")
	}
}

type Statistics struct {
	mu              sync.Mutex
	totalSent       int
	dataReceived    int
	nackReceived    int
	timeoutReceived int
	cancelReceived  int
}

func (s *Statistics) IncrementSent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalSent++
}

func (s *Statistics) IncrementData() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dataReceived++
}

func (s *Statistics) IncrementNack() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nackReceived++
}

func (s *Statistics) IncrementTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timeoutReceived++
}

func (s *Statistics) IncrementCancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelReceived++
}

func (s *Statistics) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Info(consumerTag, "=== Final Statistics ===")
	log.Info(consumerTag, "Statistics Summary",
		"totalSent", s.totalSent,
		"dataReceived", s.dataReceived,
		"nackReceived", s.nackReceived,
		"timeouts", s.timeoutReceived,
		"cancelled", s.cancelReceived)

	if s.totalSent > 0 {
		successRate := float64(s.dataReceived) / float64(s.totalSent) * 100
		log.Info(consumerTag, "Success Rate", "rate", successRate, "unit", "%")
	}
}

// Global RTT tracker
var globalRTTTracker = NewRTTTracker()

func extractPrefix(name enc.Name) string {
	// Extract the base prefix for RTT tracking
	// For "/mars/testingData/seq-1", we want "/mars/testingData"

	// Simple approach: extract first two components as prefix
	if len(name) >= 2 {
		// Build prefix from first 2 components
		prefix := ""
		for i := 0; i < 2 && i < len(name); i++ {
			prefix += "/" + name[i].String()
		}
		return prefix
	} else if len(name) >= 1 {
		return "/" + name[0].String()
	}
	return name.String()
}

func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int) {
	// Create unique name with timestamp and sequence number
	name, _ := enc.NameFromStr("/mars/testingData")

	// Use NewStringComponent with proper TLV type
	seqStr := fmt.Sprintf("seq-%d", sequenceNum)
	name = name.Append(enc.NewStringComponent(0x08, seqStr)) // 0x08 is GenericNameComponent

	intCfg := &ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(INTEREST_LIFETIME),
		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
	}

	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
	if err != nil {
		log.Error(consumerTag, "Unable to make Interest", "err", err, "seq", sequenceNum)
		return
	}

	stats.IncrementSent()

	// Extract prefix and record interest send time for RTT tracking
	interestName := interest.FinalName.String()
	prefix := extractPrefix(interest.FinalName)
	sendTime := time.Now()

	globalRTTTracker.RecordInterestSent(interestName, prefix, sendTime)

	log.Debug(consumerTag, "Sending Interest",
		"seq", sequenceNum,
		"name", interestName,
		"prefix", prefix)

	err = app.Express(interest,
		func(args ndn.ExpressCallbackArgs) {
			receiveTime := time.Now()

			switch args.Result {
			case ndn.InterestResultNack:
				log.Warn(consumerTag, "Interest Nacked",
					"seq", sequenceNum,
					"reason", args.NackReason)
				stats.IncrementNack()
				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

			case ndn.InterestResultTimeout:
				log.Warn(consumerTag, "Interest Timeout", "seq", sequenceNum)
				stats.IncrementTimeout()
				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

			case ndn.InterestCancelled:
				log.Info(consumerTag, "Interest Cancelled", "seq", sequenceNum)
				stats.IncrementCancel()
				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

			case ndn.InterestResultData:
				data := args.Data
				content := data.Content().Join()
				dataName := data.Name().String()

				// Deserialize the structured data packet
				dataPacket, err := datapacket.DeserializeDataPacket(content)
				if err != nil {
					log.Error(consumerTag, "Failed to deserialize data packet",
						"err", err, "seq", sequenceNum, "dataName", dataName)
					stats.IncrementData()                                // Still count as received data
					globalRTTTracker.RecordInterestTimeout(interestName) // Clean up
					return
				}

				// Record RTT measurement
				rttSample := globalRTTTracker.RecordDataReceived(dataName, prefix, receiveTime)
				currentEstimation := globalRTTTracker.GetEstimatedRTT(prefix)

				log.Info(consumerTag, "Data Received",
					"seq", sequenceNum,
					"dataName", dataName,
					"interestQsf", dataPacket.InterestQsf,
					"dataQsf", dataPacket.DataQsf,
					"payloadSize", len(content),
					"valuesCount", len(dataPacket.Values),
					"rttSample", rttSample,
					"estimatedRTT", currentEstimation,
					"unit", "ms")
				stats.IncrementData()
			}
		})

	if err != nil {
		log.Error(consumerTag, "Unable to send Interest", "err", err, "seq", sequenceNum)
		return
	}
}

func main() {
	// Set logging level - you can change this to control verbosity
	// LevelTrace (-8) - Most verbose
	// LevelDebug (-4) - Debug messages
	// LevelInfo (0)   - Info messages (default)
	// LevelWarn (4)   - Warning messages
	// LevelError (8)  - Error messages only
	// LevelFatal (12) - Fatal messages only
	log.Default().SetLevel(log.LevelInfo)

	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err := app.Start()
	if err != nil {
		log.Fatal(consumerTag, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	stats := &Statistics{}

	// Calculate interval between Interests
	interval := time.Second / INTERESTS_PER_SECOND

	log.Info(consumerTag, "Starting NDN Consumer",
		"interestsPerSecond", INTERESTS_PER_SECOND,
		"totalDuration", TOTAL_DURATION,
		"interval", interval,
		"interestLifetime", INTEREST_LIFETIME,
		"ewmaFactor", EWMA_FACTOR)

	// Create ticker for sending Interests
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Create timer for total duration
	totalTimer := time.NewTimer(TOTAL_DURATION)
	defer totalTimer.Stop()

	sequenceNum := 0
	startTime := time.Now()

	log.Info(consumerTag, "Starting Interest generation loop")

	// Main sending loop
	for {
		select {
		case <-ticker.C:
			sequenceNum++
			go sendSingleInterest(app, stats, sequenceNum)

		case <-totalTimer.C:
			log.Info(consumerTag, "Total duration reached, stopping", "duration", TOTAL_DURATION)
			goto cleanup
		}
	}

cleanup:
	elapsed := time.Since(startTime)
	log.Info(consumerTag, "Consumer stopped", "elapsedTime", elapsed)

	// Wait a bit for outstanding Interests to complete
	log.Info(consumerTag, "Waiting for outstanding Interests to complete",
		"waitTime", INTEREST_LIFETIME+1*time.Second)
	time.Sleep(INTEREST_LIFETIME + 1*time.Second)

	stats.Print()
	globalRTTTracker.PrintStats()
	log.Info(consumerTag, "Consumer application finished")
}

// package main

// import (
// 	"fmt"
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
// 	INTERESTS_PER_SECOND = 10               // Rate: 10 interests/sec
// 	TOTAL_DURATION       = 10 * time.Second // Total duration: 10 seconds
// 	INTEREST_LIFETIME    = 6 * time.Second  // Individual Interest lifetime
// 	EWMA_FACTOR          = 0.1              // EWMA smoothing factor
// )

// // Create a tag for our consumer application
// type ConsumerTag struct{}

// func (ConsumerTag) String() string {
// 	return "NDN-Consumer"
// }

// var consumerTag = ConsumerTag{}

// // RTT tracking structure
// type RTTTracker struct {
// 	mu               sync.RWMutex
// 	estimatedRTT     map[string]float64   // prefix -> estimated RTT in milliseconds
// 	pendingInterests map[string]time.Time // interest name -> send time
// 	ewmaFactor       float64
// 	totalSamples     int
// 	totalRTT         float64
// }

// func NewRTTTracker() *RTTTracker {
// 	return &RTTTracker{
// 		estimatedRTT:     make(map[string]float64),
// 		pendingInterests: make(map[string]time.Time),
// 		ewmaFactor:       EWMA_FACTOR,
// 		totalSamples:     0,
// 		totalRTT:         0,
// 	}
// }

// func (rtt *RTTTracker) RecordInterestSent(interestName string, prefix string, sendTime time.Time) {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()

// 	rtt.pendingInterests[interestName] = sendTime

// 	// Initialize RTT estimation for new prefix if not exists
// 	if _, exists := rtt.estimatedRTT[prefix]; !exists {
// 		rtt.estimatedRTT[prefix] = 10.0 // Initialize with 10 milliseconds default
// 		log.Debug(consumerTag, "Initialized RTT estimation for prefix",
// 			"prefix", prefix, "initialRTT", 10.0)
// 	}
// }

// func (rtt *RTTTracker) RecordDataReceived(dataName string, prefix string, receiveTime time.Time) float64 {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()

// 	sendTime, exists := rtt.pendingInterests[dataName]
// 	if !exists {
// 		log.Warn(consumerTag, "No pending interest found for name", "name", dataName)
// 		return 0
// 	}

// 	// Calculate RTT in milliseconds
// 	rttSample := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

// 	// Update EWMA: RTT_estimation = EWMA_FACTOR * RTT_estimation + (1 - EWMA_FACTOR) * sample
// 	currentEstimation, exists := rtt.estimatedRTT[prefix]
// 	if !exists {
// 		currentEstimation = 10.0 // fallback default
// 	}

// 	newEstimation := rtt.ewmaFactor*currentEstimation + (1-rtt.ewmaFactor)*rttSample
// 	rtt.estimatedRTT[prefix] = newEstimation

// 	// Update global statistics
// 	rtt.totalSamples++
// 	rtt.totalRTT += rttSample

// 	// Clean up pending interest
// 	delete(rtt.pendingInterests, dataName)

// 	log.Debug(consumerTag, "RTT Updated",
// 		// "prefix", prefix,
// 		"interestName", dataName,
// 		"sampleRTT", rttSample,
// 		// "previousRttEstimation", currentEstimation,
// 		"newRttEstimation", newEstimation,
// 		// "ewmaFactor", rtt.ewmaFactor
// 	)

// 	return rttSample
// }

// func (rtt *RTTTracker) RecordInterestTimeout(interestName string) {
// 	rtt.mu.Lock()
// 	defer rtt.mu.Unlock()

// 	// Clean up pending interest on timeout
// 	delete(rtt.pendingInterests, interestName)
// }

// func (rtt *RTTTracker) GetEstimatedRTT(prefix string) float64 {
// 	rtt.mu.RLock()
// 	defer rtt.mu.RUnlock()

// 	estimation, exists := rtt.estimatedRTT[prefix]
// 	if !exists {
// 		return 10.0 // Default 10 milliseconds
// 	}
// 	return estimation
// }

// func (rtt *RTTTracker) GetGlobalStats() (avgRTT float64, samples int, estimations map[string]float64) {
// 	rtt.mu.RLock()
// 	defer rtt.mu.RUnlock()

// 	if rtt.totalSamples > 0 {
// 		avgRTT = rtt.totalRTT / float64(rtt.totalSamples)
// 	}

// 	// Make a copy of estimations map
// 	estimations = make(map[string]float64)
// 	for prefix, est := range rtt.estimatedRTT {
// 		estimations[prefix] = est
// 	}

// 	return avgRTT, rtt.totalSamples, estimations
// }

// func (rtt *RTTTracker) PrintStats() {
// 	avgRTT, samples, estimations := rtt.GetGlobalStats()

// 	log.Info(consumerTag, "=== RTT Statistics ===")
// 	log.Info(consumerTag, "Global RTT Stats",
// 		"avgRTT", avgRTT,
// 		"samples", samples,
// 		"unit", "ms")

// 	for prefix, estimation := range estimations {
// 		log.Info(consumerTag, "Per-Prefix RTT Estimation",
// 			"prefix", prefix,
// 			"estimatedRTT", estimation,
// 			"unit", "ms")
// 	}
// }

// type Statistics struct {
// 	mu              sync.Mutex
// 	totalSent       int
// 	dataReceived    int
// 	nackReceived    int
// 	timeoutReceived int
// 	cancelReceived  int
// }

// func (s *Statistics) IncrementSent() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.totalSent++
// }

// func (s *Statistics) IncrementData() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.dataReceived++
// }

// func (s *Statistics) IncrementNack() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.nackReceived++
// }

// func (s *Statistics) IncrementTimeout() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.timeoutReceived++
// }

// func (s *Statistics) IncrementCancel() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.cancelReceived++
// }

// func (s *Statistics) Print() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	log.Info(consumerTag, "=== Final Statistics ===")
// 	log.Info(consumerTag, "Statistics Summary",
// 		"totalSent", s.totalSent,
// 		"dataReceived", s.dataReceived,
// 		"nackReceived", s.nackReceived,
// 		"timeouts", s.timeoutReceived,
// 		"cancelled", s.cancelReceived)

// 	if s.totalSent > 0 {
// 		successRate := float64(s.dataReceived) / float64(s.totalSent) * 100
// 		log.Info(consumerTag, "Success Rate", "rate", successRate, "unit", "%")
// 	}
// }

// // Global RTT tracker
// var globalRTTTracker = NewRTTTracker()

// func extractPrefix(name enc.Name) string {
// 	// Extract the base prefix for RTT tracking
// 	// For "/mars/testingData/seq-1", we want "/mars/testingData"

// 	// Simple approach: extract first two components as prefix
// 	if len(name) >= 2 {
// 		// Build prefix from first 2 components
// 		prefix := ""
// 		for i := 0; i < 2 && i < len(name); i++ {
// 			prefix += "/" + name[i].String()
// 		}
// 		return prefix
// 	} else if len(name) >= 1 {
// 		return "/" + name[0].String()
// 	}
// 	return name.String()
// }

// func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int) {
// 	// Create unique name with timestamp and sequence number
// 	name, _ := enc.NameFromStr("/mars/testingData")

// 	// Use NewStringComponent with proper TLV type
// 	seqStr := fmt.Sprintf("seq-%d", sequenceNum)
// 	name = name.Append(enc.NewStringComponent(0x08, seqStr)) // 0x08 is GenericNameComponent

// 	intCfg := &ndn.InterestConfig{
// 		MustBeFresh: true,
// 		Lifetime:    optional.Some(INTEREST_LIFETIME),
// 		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
// 	}

// 	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
// 	if err != nil {
// 		log.Error(consumerTag, "Unable to make Interest", "err", err, "seq", sequenceNum)
// 		return
// 	}

// 	stats.IncrementSent()

// 	// Extract prefix and record interest send time for RTT tracking
// 	interestName := interest.FinalName.String()
// 	prefix := extractPrefix(interest.FinalName)
// 	sendTime := time.Now()

// 	globalRTTTracker.RecordInterestSent(interestName, prefix, sendTime)

// 	log.Debug(consumerTag, "Sending Interest",
// 		"seq", sequenceNum,
// 		"name", interestName,
// 		"prefix", prefix)

// 	err = app.Express(interest,
// 		func(args ndn.ExpressCallbackArgs) {
// 			// receiveTime := time.Now()

// 			switch args.Result {
// 			case ndn.InterestResultNack:
// 				log.Warn(consumerTag, "Interest Nacked",
// 					"seq", sequenceNum,
// 					"reason", args.NackReason)
// 				stats.IncrementNack()
// 				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

// 			case ndn.InterestResultTimeout:
// 				log.Warn(consumerTag, "Interest Timeout", "seq", sequenceNum)
// 				stats.IncrementTimeout()
// 				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

// 			case ndn.InterestCancelled:
// 				log.Info(consumerTag, "Interest Cancelled", "seq", sequenceNum)
// 				stats.IncrementCancel()
// 				globalRTTTracker.RecordInterestTimeout(interestName) // Clean up

// 			case ndn.InterestResultData:
// 				data := args.Data
// 				content := string(data.Content().Join())
// 				dataName := data.Name().String()

// 				// Record RTT measurement
// 				// rttSample := globalRTTTracker.RecordDataReceived(dataName, prefix, receiveTime)
// 				currentEstimation := globalRTTTracker.GetEstimatedRTT(prefix)

// 				log.Info(consumerTag, "Data Received",
// 					// "seq", sequenceNum,
// 					// "interestName", interestName,
// 					"dataName", dataName,
// 					"content", content,
// 					"contentLength", len(content),
// 					// "rttSample", rttSample,
// 					"estimatedRTT", currentEstimation, "ms")
// 				stats.IncrementData()
// 			}
// 		})

// 	if err != nil {
// 		log.Error(consumerTag, "Unable to send Interest", "err", err, "seq", sequenceNum)
// 		return
// 	}
// }

// func main() {
// 	// Set logging level - you can change this to control verbosity
// 	// LevelTrace (-8) - Most verbose
// 	// LevelDebug (-4) - Debug messages
// 	// LevelInfo (0)   - Info messages (default)
// 	// LevelWarn (4)   - Warning messages
// 	// LevelError (8)  - Error messages only
// 	// LevelFatal (12) - Fatal messages only
// 	log.Default().SetLevel(log.LevelInfo)

// 	app := engine.NewBasicEngine(engine.NewDefaultFace())
// 	err := app.Start()
// 	if err != nil {
// 		log.Fatal(consumerTag, "Unable to start engine", "err", err)
// 		return
// 	}
// 	defer app.Stop()

// 	stats := &Statistics{}

// 	// Calculate interval between Interests
// 	interval := time.Second / INTERESTS_PER_SECOND

// 	log.Info(consumerTag, "Starting NDN Consumer",
// 		"interestsPerSecond", INTERESTS_PER_SECOND,
// 		"totalDuration", TOTAL_DURATION,
// 		"interval", interval,
// 		"interestLifetime", INTEREST_LIFETIME,
// 		"ewmaFactor", EWMA_FACTOR)

// 	// Create ticker for sending Interests
// 	ticker := time.NewTicker(interval)
// 	defer ticker.Stop()

// 	// Create timer for total duration
// 	totalTimer := time.NewTimer(TOTAL_DURATION)
// 	defer totalTimer.Stop()

// 	sequenceNum := 0
// 	startTime := time.Now()

// 	log.Info(consumerTag, "Starting Interest generation loop")

// 	// Main sending loop
// 	for {
// 		select {
// 		case <-ticker.C:
// 			sequenceNum++
// 			go sendSingleInterest(app, stats, sequenceNum)

// 		case <-totalTimer.C:
// 			log.Info(consumerTag, "Total duration reached, stopping", "duration", TOTAL_DURATION)
// 			goto cleanup
// 		}
// 	}

// cleanup:
// 	elapsed := time.Since(startTime)
// 	log.Info(consumerTag, "Consumer stopped", "elapsedTime", elapsed)

// 	// Wait a bit for outstanding Interests to complete
// 	log.Info(consumerTag, "Waiting for outstanding Interests to complete",
// 		"waitTime", INTEREST_LIFETIME+1*time.Second)
// 	time.Sleep(INTEREST_LIFETIME + 1*time.Second)

// 	stats.Print()
// 	globalRTTTracker.PrintStats()
// 	log.Info(consumerTag, "Consumer application finished")
// }
