package main

import (
	"fmt"
	"ndnd-mars-three/internal/datapacket"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/object"         // Imported for object.NewClient
	"github.com/named-data/ndnd/std/object/storage" // Imported for storage.NewMemoryStore
	sec_pib "github.com/named-data/ndnd/std/security/pib"
	"github.com/named-data/ndnd/std/types/optional"
)

var app ndn.Engine
var pib *sec_pib.SqlitePib

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

func onInterest(args ndn.InterestHandlerArgs) {
	interest := args.Interest
	name := interest.Name()

	// Name structure: /dest_prefix/src_node/mode/...
	// Index 2 corresponds to the mode ("dt" or "pd")
	if len(name) < 3 {
		log.Warn(nil, "Interest name too short to determine mode", "name", name.String())
		return
	}

	mode := name.At(2).String()

	// TODO: added by yitong, nack pipeline
	//! This should be removed later
	// // Check if this is seq-15000 in dt mode - send NACK for testing
	// if mode == "dt" {
	// 	seqNum := extractSeqNum(name.String())
	// 	if seqNum == 15000 {
	// 		log.Info(nil, "Sending NACK for seq-15000 (testing)", "name", name.String())

	// 		// Create an LpPacket with NACK indication
	// 		// The Fragment should contain the Interest packet
	// 		interestWire := args.RawInterest
	// 		if len(interestWire) == 0 {
	// 			// If RawInterest is empty, we need to encode the interest
	// 			// For now, we'll use the wire from the interest if available
	// 			log.Warn(nil, "RawInterest is empty, cannot create NACK")
	// 			return
	// 		}

	// 		// Create LpPacket with NACK
	// 		lpPkt := &spec.Packet{
	// 			LpPacket: &spec.LpPacket{
	// 				Fragment: interestWire,
	// 				Nack: &spec.NetworkNack{
	// 					Reason: spec.NackReasonNoRoute, // Use NoRoute reason for testing
	// 				},
	// 				PitToken: args.PitToken,
	// 			},
	// 		}

	// 		// Encode the LpPacket
	// 		encoder := spec.PacketEncoder{}
	// 		encoder.Init(lpPkt)
	// 		nackWire := encoder.Encode(lpPkt)

	// 		if nackWire == nil {
	// 			log.Error(nil, "Failed to encode NACK packet")
	// 			return
	// 		}

	// 		// Send NACK directly via the engine's face
	// 		// We need to access the face from the engine
	// 		if basicEngine, ok := app.(*engine_basic.Engine); ok {
	// 			err := basicEngine.Face().Send(nackWire)
	// 			if err != nil {
	// 				log.Error(nil, "Failed to send NACK", "err", err)
	// 			} else {
	// 				log.Info(nil, "NACK sent successfully", "name", name.String(), "reason", spec.NackReasonNoRoute)
	// 			}
	// 		} else {
	// 			log.Error(nil, "Cannot access engine face to send NACK")
	// 		}
	// 		return
	// 	}
	// }

	var content []byte
	var err error

	// Branch logic based on mode
	switch mode {
	case "pd":
		// Path Discovery Mode
		// Requirement: TierConverged = false, NodeConverged = true
		// Payload: None (handled by NewDiscoveryPacket structure)
		discoveryPacket := datapacket.NewDiscoveryPacket(true, true)

		// fmt.Printf(">> PD Interest: %s | Replying: %s\n", name.String(), discoveryPacket.String())

		content, err = discoveryPacket.SerializeDiscovery()
		if err != nil {
			log.Error(nil, "Unable to serialize discovery packet", "err", err)
			return
		}

	case "dt":
		// Data Transmission Mode
		// Default behavior: Zeroed values
		dataPacket := datapacket.NewDataPacket()

		// fmt.Printf(">> DT Interest: %s | Replying: %s\n", name.String(), dataPacket.String())

		content, err = dataPacket.SerializeData()
		if err != nil {
			log.Error(nil, "Unable to serialize data packet", "err", err)
			return
		}

	default:
		log.Warn(nil, "Unknown interest mode", "mode", mode, "name", name.String())
		return
	}

	// Create NDN Data packet
	data, err := app.Spec().MakeData(
		interest.Name(),
		&ndn.DataConfig{
			ContentType: optional.Some(ndn.ContentTypeBlob),
			Freshness:   optional.Some(10 * time.Second),
		},
		enc.Wire{content},
		nil, // Use default signer
	)

	if err != nil {
		log.Error(nil, "Unable to encode data", "err", err)
		return
	}

	err = args.Reply(data.Wire)
	if err != nil {
		log.Error(nil, "Unable to reply with data", "err", err)
		return
	}
}

func main() {
	// 1. Validate Command Line Arguments
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <prefix>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /mars\n", os.Args[0])
		os.Exit(1)
	}

	// Get the prefix string from the first argument
	inputPrefix := os.Args[1]

	app = engine.NewBasicEngine(engine.NewDefaultFace())
	err := app.Start()
	if err != nil {
		log.Fatal(nil, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	// Initialize the object client using the correct package and memory storage
	client := object.NewClient(app, storage.NewMemoryStore(), nil)
	if err = client.Start(); err != nil {
		log.Error(nil, "Unable to start object client", "err", err)
		return
	}
	defer client.Stop()

	homedir, _ := os.UserHomeDir()
	tpm := sec_pib.NewFileTpm(filepath.Join(homedir, ".ndn/ndnsec-key-file"))
	pib = sec_pib.NewSqlitePib(filepath.Join(homedir, ".ndn/pib.db"), tpm)

	// 2. Convert the input string to an NDN Name
	prefix, err := enc.NameFromStr(inputPrefix)
	if err != nil {
		log.Fatal(nil, "Invalid prefix format", "err", err, "input", inputPrefix)
		return
	}

	// Attach the local handler for custom processing
	err = app.AttachHandler(prefix, onInterest)
	if err != nil {
		log.Error(nil, "Unable to register handler", "err", err)
		return
	}

	// Use the Client to announce the prefix with Expose=true
	fmt.Printf("Announcing prefix %s with Expose=true ...\n", prefix.String())
	client.AnnouncePrefix(ndn.Announcement{
		Name:   prefix,
		Expose: true,
		OnError: func(err error) {
			log.Error(nil, "Prefix announcement failed", "err", err)
		},
	})

	fmt.Printf("Start serving on prefix: %s ...\n", prefix.String())
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
	receivedSig := <-sigChannel
	log.Info(nil, "Received signal - exiting", "signal", receivedSig)
}
