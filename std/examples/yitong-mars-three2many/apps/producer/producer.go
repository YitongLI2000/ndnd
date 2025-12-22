package main

import (
	"fmt"
	"ndnd-mars-three/internal/datapacket"
	"os"
	"os/signal"
	"path/filepath"
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
	var content []byte
	var err error

	// Branch logic based on mode
	switch mode {
	case "pd":
		// Path Discovery Mode
		// Requirement: TierConverged = false, NodeConverged = true
		// Payload: None (handled by NewDiscoveryPacket structure)
		discoveryPacket := datapacket.NewDiscoveryPacket(false, true)

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

// package main

// import (
// 	"fmt"
// 	"ndnd-mars-three/internal/datapacket"
// 	"os"
// 	"os/signal"
// 	"path/filepath"
// 	"syscall"
// 	"time"

// 	enc "github.com/named-data/ndnd/std/encoding"
// 	"github.com/named-data/ndnd/std/engine"
// 	"github.com/named-data/ndnd/std/log"
// 	"github.com/named-data/ndnd/std/ndn"
// 	"github.com/named-data/ndnd/std/object"         // Imported for object.NewClient
// 	"github.com/named-data/ndnd/std/object/storage" // Imported for storage.NewMemoryStore
// 	sec_pib "github.com/named-data/ndnd/std/security/pib"
// 	"github.com/named-data/ndnd/std/types/optional"
// )

// var app ndn.Engine
// var pib *sec_pib.SqlitePib

// func onInterest(args ndn.InterestHandlerArgs) {
// 	interest := args.Interest

// 	// fmt.Printf(">> I: %s\n", interest.Name().String())

// 	// Create structured data packet
// 	dataPacket := datapacket.NewDataPacket()

// 	// Serialize the data packet
// 	content, err := dataPacket.SerializeData()
// 	if err != nil {
// 		log.Error(nil, "Unable to serialize data packet", "err", err)
// 		return
// 	}

// 	// Create NDN Data packet
// 	data, err := app.Spec().MakeData(
// 		interest.Name(),
// 		&ndn.DataConfig{
// 			ContentType: optional.Some(ndn.ContentTypeBlob),
// 			Freshness:   optional.Some(10 * time.Second),
// 		},
// 		enc.Wire{content},
// 		nil, // Use default signer
// 	)

// 	if err != nil {
// 		log.Error(nil, "Unable to encode data", "err", err)
// 		return
// 	}

// 	err = args.Reply(data.Wire)
// 	if err != nil {
// 		log.Error(nil, "Unable to reply with data", "err", err)
// 		return
// 	}

// 	// fmt.Printf("<< D: %s\n", interest.Name().String())
// 	// fmt.Printf("Content: %s (size: %d bytes)\n", dataPacket.String(), len(content))
// 	// fmt.Printf("\n")
// }

// func main() {
// 	// 1. Validate Command Line Arguments
// 	if len(os.Args) < 2 {
// 		fmt.Fprintf(os.Stderr, "Usage: %s <prefix>\n", os.Args[0])
// 		fmt.Fprintf(os.Stderr, "Example: %s /mars\n", os.Args[0])
// 		os.Exit(1)
// 	}

// 	// Get the prefix string from the first argument
// 	inputPrefix := os.Args[1]

// 	app = engine.NewBasicEngine(engine.NewDefaultFace())
// 	err := app.Start()
// 	if err != nil {
// 		log.Fatal(nil, "Unable to start engine", "err", err)
// 		return
// 	}
// 	defer app.Stop()

// 	// Initialize the object client using the correct package and memory storage
// 	client := object.NewClient(app, storage.NewMemoryStore(), nil)
// 	if err = client.Start(); err != nil {
// 		log.Error(nil, "Unable to start object client", "err", err)
// 		return
// 	}
// 	defer client.Stop()

// 	homedir, _ := os.UserHomeDir()
// 	tpm := sec_pib.NewFileTpm(filepath.Join(homedir, ".ndn/ndnsec-key-file"))
// 	pib = sec_pib.NewSqlitePib(filepath.Join(homedir, ".ndn/pib.db"), tpm)

// 	// 2. Convert the input string to an NDN Name
// 	prefix, err := enc.NameFromStr(inputPrefix)
// 	if err != nil {
// 		log.Fatal(nil, "Invalid prefix format", "err", err, "input", inputPrefix)
// 		return
// 	}

// 	// Attach the local handler for custom processing
// 	err = app.AttachHandler(prefix, onInterest)
// 	if err != nil {
// 		log.Error(nil, "Unable to register handler", "err", err)
// 		return
// 	}

// 	// Use the Client to announce the prefix with Expose=true
// 	fmt.Printf("Announcing prefix %s with Expose=true ...\n", prefix.String())
// 	client.AnnouncePrefix(ndn.Announcement{
// 		Name:   prefix,
// 		Expose: true,
// 		OnError: func(err error) {
// 			log.Error(nil, "Prefix announcement failed", "err", err)
// 		},
// 	})

// 	fmt.Printf("Start serving on prefix: %s ...\n", prefix.String())
// 	sigChannel := make(chan os.Signal, 1)
// 	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
// 	receivedSig := <-sigChannel
// 	log.Info(nil, "Received signal - exiting", "signal", receivedSig)
// }
