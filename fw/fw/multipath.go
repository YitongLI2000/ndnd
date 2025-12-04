/* Multipath Strategy - MARS project
 *
 * Implemented by Yitong, 2025.12.04
 * Modified for Round-Robin, 2025.12.04
 * Description: Round-Robin forwarding across all available nexthops.
 * No queues, no schedulers, fully thread-safe using atomic counters.
 */

package fw

import (
	"sort"
	"sync/atomic"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
)

type Multipath struct {
	StrategyBase
	// rrCounter is a global counter for this strategy instance.
	// We use atomic operations to ensure thread safety if the strategy
	// is shared across routines.
	rrCounter uint64
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
	StrategyVersions["multipath"] = []uint64{1}
}

func (s *Multipath) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "multipath", 1)
}

func (s *Multipath) AfterContentStoreHit(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	s.SendData(packet, pitEntry, inFace, 0)
}

func (s *Multipath) AfterReceiveData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	for faceID := range pitEntry.InRecords() {
		s.SendData(packet, pitEntry, faceID, inFace)
	}
}

func (s *Multipath) AfterReceiveInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Log the incoming Interest
	core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String(), "Interest Name: ", packet.Name)

	if len(nexthops) == 0 {
		core.Log.Warn(s, "No nexthops available for", packet.Name)
		return
	}

	// 1. Sort Nexthops to ensure deterministic order for the Round-Robin.
	// Without sorting, the order of nexthops might fluctuate, making RR behave randomly.
	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
	copy(sortedHops, nexthops)

	sort.Slice(sortedHops, func(i, j int) bool {
		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
	})

	// 2. Atomic Round-Robin Selection
	// We atomically increment the counter. The result is unique for this call.
	// We subtract 1 because AddUint64 returns the new value, and we want 0-based indexing logic start.
	currentCount := atomic.AddUint64(&s.rrCounter, 1) - 1

	// Calculate index using Modulo
	selectedIndex := currentCount % uint64(len(sortedHops))
	targetFace := sortedHops[selectedIndex].Nexthop

	// 3. Log the decision
	core.Log.Info(s, "Round-Robin Forwarding",
		"Name", packet.Name,
		"Total_Hops", len(sortedHops),
		"Selected_Index", selectedIndex,
		"Target_Face", targetFace)

	// 4. Send Immediately
	s.SendInterest(packet, pitEntry, targetFace, inFace)
}

func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// /* Multipath Strategy - MARS project
//  *
//  * Implemented by Yitong, 2025.12.04
//  * Description: Static binding of /proXapp prefixes to specific nexthops.
//  * No queues, no schedulers, fully thread-safe.
//  */

// package fw

// import (
// 	"sort"
// 	"strconv"
// 	"strings"

// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// )

// type Multipath struct {
// 	StrategyBase
// }

// func init() {
// 	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
// 	StrategyVersions["multipath"] = []uint64{1}
// }

// func (s *Multipath) Instantiate(fwThread *Thread) {
// 	s.NewStrategyBase(fwThread, "multipath", 1)
// }

// func (s *Multipath) AfterContentStoreHit(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	s.SendData(packet, pitEntry, inFace, 0)
// }

// func (s *Multipath) AfterReceiveData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	for faceID := range pitEntry.InRecords() {
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// func (s *Multipath) AfterReceiveInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	if len(nexthops) == 0 {
// 		return
// 	}

// 	// 1. Sort Nexthops to ensure deterministic mapping.
// 	// If we have Face 260, 265, 270, we want index 0 to always be 260.
// 	// We create a slice copy to avoid modifying the original FIB entry order.
// 	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
// 	copy(sortedHops, nexthops)

// 	sort.Slice(sortedHops, func(i, j int) bool {
// 		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
// 	})

// 	// 2. Determine the index based on the Prefix Name
// 	// Expected format: /pro{N}app/...
// 	targetIndex := 0

// 	if len(packet.Name) > 0 {
// 		firstComp := packet.Name[0].String() // e.g., "pro2app"

// 		// Simple parsing logic: extract the digit
// 		// This assumes the digit is always at index 3 (p-r-o-X-a-p-p)
// 		if strings.HasPrefix(firstComp, "pro") && len(firstComp) >= 4 {
// 			// Extract the character at index 3 (the number)
// 			digitStr := string(firstComp[3])
// 			if val, err := strconv.Atoi(digitStr); err == nil {
// 				targetIndex = val
// 			}
// 		}
// 	}

// 	// 3. Map Index to Face
// 	// We use Modulo (%) operator.
// 	// If we have 5 prefixes (0-4) and 5 faces, it maps 1:1.
// 	// If one face dies and we only have 4 faces left, pro4app will wrap around to face 0.
// 	selectedHopIndex := targetIndex % len(sortedHops)
// 	targetFace := sortedHops[selectedHopIndex].Nexthop

// 	// Optional: Logging for verification
// 	// core.Log.Info(s, "Forwarding", "name", packet.Name, "mapped_index", targetIndex, "target_face", targetFace)

// 	// 4. Send Immediately (BestRoute style)
// 	s.SendInterest(packet, pitEntry, targetFace, inFace)
// }

// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// /* Multipath forwarding strategy - MARS project
//  *
//  * Implemented by Yitong, 2025.12.3
//  * Email: yliop@connect.hkust-gz.edu.cn
//  */

// package fw

// import (
// 	"math/rand"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/named-data/ndnd/fw/core"
// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// 	enc "github.com/named-data/ndnd/std/encoding"
// 	"github.com/named-data/ndnd/std/types/optional"
// )

// // Global parameters for the Multipath strategy
// const (
// 	// QueueSize defines the finite size of the FIFO interest queue per prefix.
// 	QueueSize = 100
// 	// SendRateInterval defines the static interest sending rate (200 packets/s => 5ms).
// 	SendRateInterval = 5 * time.Millisecond
// 	// TickMarkerComponent is the name component used to identify internal tick interests.
// 	TickMarkerComponent = "MULTIPATH-TICK"
// )

// // PendingInterest holds the context required to send an Interest later.
// type PendingInterest struct {
// 	Packet   *defn.Pkt
// 	PitEntry table.PitEntry
// 	InFace   uint64
// }

// // SharedState holds the data structures shared across all strategy instances (threads).
// type SharedState struct {
// 	// Mutex to protect concurrent access to the maps below
// 	mu sync.Mutex

// 	// Map 1: Per-prefix Interest Queue
// 	// Key: Prefix (First Component String), Value: Buffered Channel of PendingInterest
// 	queues map[string]chan PendingInterest

// 	// Map 2: Face -> List of Prefixes
// 	// Key: Nexthop FaceID, Value: List of prefixes (Strings) mapped to this face
// 	facePrefixes map[uint64][]string

// 	// Map 3: Face -> Round Robin Index
// 	// Key: Nexthop FaceID, Value: Index of the next prefix to serve in facePrefixes list
// 	faceRRIndex map[uint64]int

// 	// Tracks which faces have an active scheduler goroutine running
// 	activeFaces map[uint64]bool
// }

// // globalState is the singleton instance shared by all Multipath strategy objects.
// var globalState = &SharedState{
// 	queues:       make(map[string]chan PendingInterest),
// 	facePrefixes: make(map[uint64][]string),
// 	faceRRIndex:  make(map[uint64]int),
// 	activeFaces:  make(map[uint64]bool),
// }

// // Multipath is a forwarding strategy based on BestRoute.
// // It implements per-prefix buffering and per-face round-robin scheduling.
// type Multipath struct {
// 	StrategyBase
// }

// // Registers the Multipath strategy with version 1 in the strategy registry.
// func init() {
// 	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
// 	StrategyVersions["multipath"] = []uint64{1}
// }

// // Initializes the *Multipath strategy by setting up its base.
// func (s *Multipath) Instantiate(fwThread *Thread) {
// 	s.NewStrategyBase(fwThread, "multipath", 1)
// 	// Note: We do NOT re-initialize globalState here, or we would wipe it for other threads.
// }

// // Sends a cached Data packet back to the requester via the specified PIT entry.
// func (s *Multipath) AfterContentStoreHit(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// ) {
// 	core.Log.Trace(s, "AfterContentStoreHit", "name", packet.Name, "faceid", inFace)
// 	s.SendData(packet, pitEntry, inFace, 0) // 0 indicates ContentStore is source
// }

// // Forwards a received Data packet to every face recorded in the PIT entry.
// func (s *Multipath) AfterReceiveData(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// ) {
// 	core.Log.Trace(s, "AfterReceiveData", "name", packet.Name, "inrecords", len(pitEntry.InRecords()))
// 	for faceID := range pitEntry.InRecords() {
// 		core.Log.Trace(s, "Forwarding Data", "name", packet.Name, "faceid", faceID)
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// // AfterReceiveInterest handles incoming Interests.
// // It distinguishes between "Real" interests (buffered) and "Tick" interests (trigger scheduling).
// func (s *Multipath) AfterReceiveInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	// 0. Check if this is a "Tick" Interest generated by our scheduler
// 	if s.isTickInterest(packet.Name) {
// 		s.processTick(packet.Name)
// 		return // Stop processing, do not forward the tick
// 	}

// 	// Normal Interest Processing
// 	if len(nexthops) == 0 {
// 		core.Log.Debug(s, "No nexthop found - DROP", "name", packet.Name)
// 		return
// 	}

// 	// Logging
// 	core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String())
// 	if strings.Contains(s.StrategyNodeName.String(), "con0") && strings.HasPrefix(packet.Name.String(), "/pro") {
// 		core.Log.Info(s, "FIB Inspection", "prefix", packet.Name, "nexthop_count", len(nexthops))
// 		for _, nh := range nexthops {
// 			core.Log.Info(s, " - FIB Entry", "nexthop_face", nh.Nexthop, "cost", nh.Cost)
// 		}
// 	}

// 	// Determine the prefix key (First Component only)
// 	var prefix string
// 	if len(packet.Name) > 0 {
// 		prefix = packet.Name[:1].String()
// 	} else {
// 		prefix = "/"
// 	}

// 	// Use Global State Lock
// 	globalState.mu.Lock()
// 	defer globalState.mu.Unlock()

// 	// 1) & 2) Check if it's a new prefix. If so, create queue and map to faces.
// 	if _, exists := globalState.queues[prefix]; !exists {
// 		core.Log.Info(s, "New Prefix Detected - Creating Queue", "prefix", prefix)

// 		// Create FIFO interest queue
// 		globalState.queues[prefix] = make(chan PendingInterest, QueueSize)

// 		// Add prefix to nexthop lists
// 		for _, nh := range nexthops {
// 			targetFaceID := nh.Nexthop

// 			globalState.facePrefixes[targetFaceID] = append(globalState.facePrefixes[targetFaceID], prefix)

// 			// 4) Ensure a scheduler is running for this SPECIFIC network face
// 			if !globalState.activeFaces[targetFaceID] {
// 				globalState.activeFaces[targetFaceID] = true
// 				globalState.faceRRIndex[targetFaceID] = 0

// 				// Start the scheduler passing ONLY the nexthop ID.
// 				go s.runFaceScheduler(targetFaceID)

// 				core.Log.Info(s, "Started Scheduler", "faceid", targetFaceID)
// 			}
// 		}
// 	}

// 	// 3) Buffer the interest into the queue
// 	queue := globalState.queues[prefix]
// 	select {
// 	case queue <- PendingInterest{Packet: packet, PitEntry: pitEntry, InFace: inFace}:
// 		core.Log.Trace(s, "Buffered Interest", "prefix", prefix, "queue_len", len(queue))
// 	default:
// 		core.Log.Warn(s, "Interest Queue Full - DROP", "prefix", prefix)
// 	}
// }

// // runFaceScheduler runs a ticker in a background goroutine.
// // It sends a "Tick" Interest to the main thread to trigger safe sending.
// func (s *Multipath) runFaceScheduler(targetFaceID uint64) {
// 	ticker := time.NewTicker(SendRateInterval)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		// Construct Name: <StrategyPrefix>/MULTIPATH-TICK/<FaceID>
// 		tickName := s.StrategyNodeName.
// 			Append(enc.NewGenericComponent(TickMarkerComponent)).
// 			Append(enc.NewGenericComponent(strconv.FormatUint(targetFaceID, 10)))

// 		nonce := rand.Uint32()

// 		fwInterest := &defn.FwInterest{
// 			NameV:  tickName,
// 			NonceV: optional.Some(nonce),
// 		}

// 		l3Pkt := &defn.FwPacket{
// 			Interest: fwInterest,
// 		}

// 		pkt := &defn.Pkt{
// 			Name:           tickName,
// 			IncomingFaceID: targetFaceID,
// 			L3:             l3Pkt,
// 		}

// 		// Queue directly to the thread's pending channel.
// 		// Note: s.thread refers to the thread that created this Strategy instance.
// 		// Even if multiple threads exist, queuing to *any* thread is sufficient to get back into the event loop.
// 		s.thread.QueueInterest(pkt)
// 	}
// }

// // isTickInterest checks if the interest is an internal trigger.
// func (s *Multipath) isTickInterest(name enc.Name) bool {
// 	if len(name) < 2 {
// 		return false
// 	}
// 	return name[len(name)-2].String() == TickMarkerComponent
// }

// // processTick executes the Round-Robin logic on the MAIN THREAD.
// func (s *Multipath) processTick(name enc.Name) {
// 	faceIDStr := name[len(name)-1].String()
// 	faceID, err := strconv.ParseUint(faceIDStr, 10, 64)
// 	if err != nil {
// 		core.Log.Error(s, "Invalid Tick FaceID", "str", faceIDStr)
// 		return
// 	}

// 	globalState.mu.Lock()
// 	prefixes, ok := globalState.facePrefixes[faceID]
// 	if !ok || len(prefixes) == 0 {
// 		globalState.mu.Unlock()
// 		return
// 	}

// 	// 4) Round-Robin Scheduling Logic
// 	startIdx := globalState.faceRRIndex[faceID]
// 	count := len(prefixes)
// 	var interestToSend *PendingInterest

// 	for i := 0; i < count; i++ {
// 		idx := (startIdx + i) % count
// 		pName := prefixes[idx]

// 		if q, qExists := globalState.queues[pName]; qExists {
// 			select {
// 			case item := <-q:
// 				interestToSend = &item
// 				globalState.faceRRIndex[faceID] = (idx + 1) % count
// 				goto Found
// 			default:
// 				// Empty queue, continue
// 			}
// 		}
// 	}

// Found:
// 	globalState.mu.Unlock()

// 	if interestToSend != nil {
// 		if time.Now().Before(interestToSend.PitEntry.ExpirationTime()) {
// 			core.Log.Trace(s, "Scheduler Sending Interest", "faceid", faceID, "name", interestToSend.Packet.Name)
// 			s.SendInterest(interestToSend.Packet, interestToSend.PitEntry, faceID, interestToSend.InFace)
// 		} else {
// 			core.Log.Debug(s, "Buffered Interest Expired - DROP", "name", interestToSend.Packet.Name)
// 		}
// 	}
// }

// // No-op; the Multipath strategy performs no action before satisfying an Interest.
// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {
// 	// This does nothing in Multipath
// }

// /* Multipath forwarding strategy - MARS project
//  *
//  * Implemented by Yitong, 2025.12.3
//  * Email: yliop@connect.hkust-gz.edu.cn
//  */

// package fw

// import (
// 	"sort"
// 	"strings"
// 	"time"

// 	"github.com/named-data/ndnd/fw/core"
// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// )

// // MultipathSuppressionTime is the time to suppress retransmissions of the same Interest.
// const MultipathSuppressionTime = 400 * time.Millisecond

// // Multipath is a forwarding strategy based on BestRoute.
// // Currently, it forwards Interests to the nexthop with the lowest cost.
// type Multipath struct {
// 	StrategyBase
// }

// // Registers the Multipath strategy with version 1 in the strategy registry.
// func init() {
// 	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
// 	StrategyVersions["multipath"] = []uint64{1}
// }

// // Initializes the *Multipath strategy by setting up its base with the name "multipath".
// func (s *Multipath) Instantiate(fwThread *Thread) {
// 	s.NewStrategyBase(fwThread, "multipath", 1)
// }

// // Sends a cached Data packet back to the requester via the specified PIT entry.
// func (s *Multipath) AfterContentStoreHit(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// ) {
// 	core.Log.Trace(s, "AfterContentStoreHit", "name", packet.Name, "faceid", inFace)
// 	s.SendData(packet, pitEntry, inFace, 0) // 0 indicates ContentStore is source
// }

// // Forwards a received Data packet to every face recorded in the PIT entry.
// func (s *Multipath) AfterReceiveData(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// ) {
// 	core.Log.Trace(s, "AfterReceiveData", "name", packet.Name, "inrecords", len(pitEntry.InRecords()))
// 	for faceID := range pitEntry.InRecords() {
// 		core.Log.Trace(s, "Forwarding Data", "name", packet.Name, "faceid", faceID)
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// // Forwards an incoming Interest. Currently identical to BestRoute logic.
// func (s *Multipath) AfterReceiveInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	if len(nexthops) == 0 {
// 		core.Log.Debug(s, "No nexthop found - DROP", "name", packet.Name)
// 		return
// 	}

// 	// StrategyNodeName is enc.Name, so we use .String() for logging
// 	core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String())

// 	// Added logging for con0 and /pro prefix to inspect FIB details
// 	if strings.Contains(s.StrategyNodeName.String(), "con0") && strings.HasPrefix(packet.Name.String(), "/pro") {
// 		core.Log.Info(s, "FIB Inspection", "prefix", packet.Name, "nexthop_count", len(nexthops))
// 		for _, nh := range nexthops {
// 			core.Log.Info(s, " - FIB Entry", "nexthop_face", nh.Nexthop, "cost", nh.Cost)
// 		}
// 	}

// 	// Sort nexthops by cost and send to best-possible nexthop
// 	sort.Slice(nexthops, func(i, j int) bool { return nexthops[i].Cost < nexthops[j].Cost })

// 	now := time.Now()
// 	for pass := range 2 {
// 		for _, nh := range nexthops {
// 			// In the first pass, skip hops that already have a out record
// 			if pass == 0 {
// 				if oR := pitEntry.OutRecords()[nh.Nexthop]; oR != nil {
// 					// Suppress retransmissions of the same Interest within suppression time
// 					if oR.LatestTimestamp.Add(MultipathSuppressionTime).After(now) {
// 						core.Log.Debug(s, "Suppressed Interest - DROP", "name", packet.Name)
// 						return
// 					}

// 					// If an out record exists, skip this hop
// 					continue
// 				}
// 			}

// 			// For the second pass, we should ideally use the least recently tried hop.
// 			// But then we need to resort the list - this is just faster for now.
// 			// In densely connected networks, this is not a big deal.

// 			core.Log.Trace(s, "Forwarding Interest", "name", packet.Name, "faceid", nh.Nexthop)
// 			if sent := s.SendInterest(packet, pitEntry, nh.Nexthop, inFace); sent {
// 				return
// 			}
// 		}
// 	}

// 	core.Log.Debug(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
// }

// // No-op; the Multipath strategy performs no action before satisfying an Interest.
// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {
// 	// This does nothing in Multipath
// }
