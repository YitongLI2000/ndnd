/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package fw

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
)

// BestRouteSuppressionTime is the time to suppress retransmissions of the same Interest.
const BestRouteSuppressionTime = 400 * time.Millisecond

// BestRoute is a forwarding strategy that forwards Interests
// to the nexthop with the lowest cost.
type BestRoute struct {
	StrategyBase
}

// (AI GENERATED DESCRIPTION): Registers the BestRoute strategy with version 1 in the strategy registry by appending its constructor to the init list.
func init() {
	strategyInit = append(strategyInit, func() Strategy { return &BestRoute{} })
	StrategyVersions["best-route"] = []uint64{1}
}

// (AI GENERATED DESCRIPTION): Initializes the *BestRoute strategy by setting up its base with the name “best‑route” and priority 1 on the provided forwarding thread.
func (s *BestRoute) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "best-route", 1)
}

// (AI GENERATED DESCRIPTION): Sends a cached Data packet (retrieved from the Content Store) back to the requester via the specified PIT entry, using the requesting face and indicating the Content Store as the data source.
func (s *BestRoute) AfterContentStoreHit(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
) {
	core.Log.Trace(s, "AfterContentStoreHit", "name", packet.Name, "faceid", inFace)
	s.SendData(packet, pitEntry, inFace, 0) // 0 indicates ContentStore is source
}

// (AI GENERATED DESCRIPTION): Forwards a received Data packet to every face recorded in the PIT entry, logging each forwarding step and invoking SendData for each destination.
func (s *BestRoute) AfterReceiveData(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
) {
	core.Log.Trace(s, "AfterReceiveData", "name", packet.Name, "inrecords", len(pitEntry.InRecords()))

	// Extract sequence number from data name and log every 200 packets
	dataName := packet.Name.String()
	seqNum := extractSeqNum(dataName)
	if seqNum >= 0 && seqNum%200 == 0 {
		prefixKey := getPrefixKey(packet.Name)
		core.Log.Debug(s, "Data packet logged", "name", packet.Name, "prefix", prefixKey, "seq", seqNum)
	}

	for faceID := range pitEntry.InRecords() {
		core.Log.Trace(s, "Forwarding Data", "name", packet.Name, "faceid", faceID)
		s.SendData(packet, pitEntry, faceID, inFace)
	}
}

// (AI GENERATED DESCRIPTION): Forwards an incoming Interest to the lowest‑cost next‑hop, suppressing retransmissions within a set time window, and drops the Interest if no usable nexthop is found.
func (s *BestRoute) AfterReceiveInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Extract sequence number from interest name and log every 200 packets
	interestName := packet.Name.String()
	seqNum := extractSeqNum(interestName)
	if seqNum >= 0 && seqNum%200 == 0 {
		prefixKey := getPrefixKey(packet.Name)
		core.Log.Debug(s, "Interest packet logged", "name", packet.Name, "prefix", prefixKey, "seq", seqNum)
	}

	if len(nexthops) == 0 {
		// core.Log.Debug(s, "No nexthop found - DROP", "name", packet.Name)
		core.Log.Trace(s, "No nexthop found - DROP", "name", packet.Name)
		return
	}

	//TODO: added by Yitong
	// Added logging for con0 and /pro prefix to inspect FIB details
	// We convert StrategyNodeName to string to check if it contains "con0"
	// Note: enc.Name.String() usually returns a URI format like "/con0", so we check if it contains the name.
	// if strings.Contains(s.StrategyNodeName.String(), "con0") && strings.HasPrefix(packet.Name.String(), "/pro") {
	// 	core.Log.Info(s, "FIB Inspection", "prefix", packet.Name, "nexthop_count", len(nexthops))
	// 	for _, nh := range nexthops {
	// 		core.Log.Info(s, " - FIB Entry", "nexthop_face", nh.Nexthop, "cost", nh.Cost)
	// 	}
	// }

	// StrategyNodeName is enc.Name, so we use .String() for logging
	// core.Log.Info(s, "Processing Interest", "NDN forwarder: ", s.StrategyNodeName.String(), "Interest Name: ", packet.Name.String())

	// Sort nexthops by cost and send to best-possible nexthop
	sort.Slice(nexthops, func(i, j int) bool { return nexthops[i].Cost < nexthops[j].Cost })

	now := time.Now()
	for pass := range 2 {
		for _, nh := range nexthops {
			// In the first pass, skip hops that already have a out record
			if pass == 0 {
				if oR := pitEntry.OutRecords()[nh.Nexthop]; oR != nil {
					// Suppress retransmissions of the same Interest within suppression time
					if oR.LatestTimestamp.Add(BestRouteSuppressionTime).After(now) {
						// core.Log.Debug(s, "Suppressed Interest - DROP", "name", packet.Name)
						core.Log.Trace(s, "Suppressed Interest - DROP", "name", packet.Name)
						return
					}

					// If an out record exists, skip this hop
					continue
				}
			}

			// For the second pass, we should ideally use the least recently tried hop.
			// But then we need to resort the list - this is just faster for now.
			// In densely connected networks, this is not a big deal.

			core.Log.Trace(s, "Forwarding Interest", "name", packet.Name, "faceid", nh.Nexthop)
			if sent := s.SendInterest(packet, pitEntry, nh.Nexthop, inFace); sent {
				return
			}
		}
	}

	// core.Log.Debug(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
	core.Log.Trace(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
}

// (AI GENERATED DESCRIPTION): No‑op; the BestRoute strategy performs no action before satisfying an Interest.
func (s *BestRoute) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {
	// This does nothing in BestRoute
}

// TODO: added by yitong, nack pipeline
// (AI GENERATED DESCRIPTION): Forwards a received NACK to all downstream faces (except the one it came from), following standard NFD behavior.
func (s *BestRoute) AfterReceiveNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	core.Log.Trace(s, "AfterReceiveNack", "name", packet.Name, "reason", nackReason, "inFace", inFace)

	// Forward NACK to all downstream faces (in-records) except the incoming face
	for faceID := range pitEntry.InRecords() {
		if faceID != inFace {
			s.SendNack(packet, pitEntry, faceID, inFace, nackReason)
		}
	}
}

// TODO: added by yitong, looped interests pipeline
// (AI GENERATED DESCRIPTION): Handles looped interests by dropping them (default NFD behavior).
func (s *BestRoute) AfterReceiveLoopedInterest(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	core.Log.Trace(s, "AfterReceiveLoopedInterest", "name", packet.Name, "inFace", inFace)
	// Default behavior: drop the looped Interest (handled by thread.go return)
}

// getPrefixKey extracts the prefix key from a packet name.
// Uses the first component as the prefix key.
func getPrefixKey(name enc.Name) string {
	if len(name) > 0 {
		return name[0].String()
	}
	return "/"
}

// extractSeqNum extracts the sequence number from a name string.
// Expected format: /prefix/node/[pd/<index>/|dt/]seq-<num>
// Returns -1 if parsing fails.
func extractSeqNum(nameStr string) int {
	parts := strings.Split(nameStr, "/")
	if len(parts) == 0 {
		return -1
	}
	// Last component should be "seq-N"
	lastPart := parts[len(parts)-1]
	if !strings.HasPrefix(lastPart, "seq-") {
		return -1
	}
	seqStr := strings.TrimPrefix(lastPart, "seq-")
	seqNum, err := strconv.Atoi(seqStr)
	if err != nil {
		return -1
	}
	return seqNum
}
