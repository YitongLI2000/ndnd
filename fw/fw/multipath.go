/* Multipath Strategy - MARS project
 *
 * Implemented by Yitong, 2025.12.04
 * Modified for Per-Prefix Round-Robin, 2025.12.04
 * Description: Round-Robin forwarding with independent counters per prefix.
 * Robust against dynamic nexthop changes.
 */

package fw

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
)

type Multipath struct {
	StrategyBase
	// perPrefixCounters stores a pointer to a uint64 for each prefix string.
	// Key: Prefix String (e.g., "/pro0app"), Value: *uint64
	// We use sync.Map for thread-safe concurrent access without global locking.
	perPrefixCounters sync.Map
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
	// core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String(), "Interest Name: ", packet.Name)

	if len(nexthops) == 0 {
		core.Log.Warn(s, "No nexthops available for", packet.Name)
		return
	}

	// 1. Sort Nexthops to ensure deterministic order for the Round-Robin.
	// This ensures that index 0 always refers to the same face ID (as long as that face exists).
	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
	copy(sortedHops, nexthops)

	sort.Slice(sortedHops, func(i, j int) bool {
		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
	})

	// 2. Identify the Flow/Prefix
	// We use the first component (e.g., "/pro0app") as the key for the Round-Robin counter.
	// If we used the full name, every chunk would get a new counter (0), defeating RR.
	var prefixKey string
	if len(packet.Name) > 0 {
		prefixKey = packet.Name[0].String()
	} else {
		prefixKey = "/"
	}

	// 3. Retrieve or Initialize the Counter for this Prefix
	// LoadOrStore returns the existing value if present, or stores the new one.
	// We store a pointer to uint64 so we can atomically increment the value *inside* the interface{}.
	val, _ := s.perPrefixCounters.LoadOrStore(prefixKey, new(uint64))
	counterPtr := val.(*uint64)

	// 4. Atomic Round-Robin Selection
	// Increment the counter for this specific prefix.
	currentCount := atomic.AddUint64(counterPtr, 1) - 1

	// 5. Calculate Index (Robust against Link Failure)
	// We calculate the index based on the *current* number of nexthops.
	// If a link failed and len(sortedHops) decreased, the Modulo operator (%)
	// automatically wraps the high counter value into the new, smaller range.
	// e.g., Counter=5, Hops=5 -> Index=0.
	//       Link fails, Hops=4. Next packet Counter=6 -> Index=2. Valid.
	selectedIndex := currentCount % uint64(len(sortedHops))
	targetFace := sortedHops[selectedIndex].Nexthop

	// 6. Log the decision
	// core.Log.Info(s, "Per-Prefix RR Forwarding",
	// 	"Prefix", prefixKey,
	// 	"Total_Hops", len(sortedHops),
	// 	"Selected_Index", selectedIndex,
	// 	"Target_Face", targetFace)

	// 7. Send Immediately
	s.SendInterest(packet, pitEntry, targetFace, inFace)
}

func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// /* Multipath Strategy - MARS project
//  *
//  * Implemented by Yitong, 2025.12.04
//  * Modified for Round-Robin, 2025.12.04
//  * Description: Round-Robin forwarding across all available nexthops.
//  * No queues, no schedulers, fully thread-safe using atomic counters.
//  */

// package fw

// import (
// 	"sort"
// 	"sync/atomic"

// 	"github.com/named-data/ndnd/fw/core"
// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// )

// type Multipath struct {
// 	StrategyBase
// 	// rrCounter is a global counter for this strategy instance.
// 	// We use atomic operations to ensure thread safety if the strategy
// 	// is shared across routines.
// 	rrCounter uint64
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
// 	// Log the incoming Interest
// 	// core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String(), "Interest Name: ", packet.Name)

// 	if len(nexthops) == 0 {
// 		core.Log.Warn(s, "No nexthops available for", packet.Name)
// 		return
// 	}

// 	// 1. Sort Nexthops to ensure deterministic order for the Round-Robin.
// 	// Without sorting, the order of nexthops might fluctuate, making RR behave randomly.
// 	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
// 	copy(sortedHops, nexthops)

// 	sort.Slice(sortedHops, func(i, j int) bool {
// 		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
// 	})

// 	// 2. Atomic Round-Robin Selection
// 	// We atomically increment the counter. The result is unique for this call.
// 	// We subtract 1 because AddUint64 returns the new value, and we want 0-based indexing logic start.
// 	currentCount := atomic.AddUint64(&s.rrCounter, 1) - 1

// 	// Calculate index using Modulo
// 	selectedIndex := currentCount % uint64(len(sortedHops))
// 	targetFace := sortedHops[selectedIndex].Nexthop

// 	// 3. Log the decision
// 	// core.Log.Info(s, "Round-Robin Forwarding",
// 	// 	"Name", packet.Name,
// 	// 	"Total_Hops", len(sortedHops),
// 	// 	"Selected_Index", selectedIndex,
// 	// 	"Target_Face", targetFace)

// 	// 4. Send Immediately
// 	s.SendInterest(packet, pitEntry, targetFace, inFace)
// }

// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}
