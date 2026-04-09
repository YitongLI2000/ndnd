package datapacket

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Data packet layout parameters.
// Change DataPacketValueCount to adjust DT payload vector length globally.
const (
	DataPacketValueCount = 750
	float64SizeBytes     = 8
	dataPacketMetaFields = 2 // InterestQsf + DataQsf
)

// -----------------------------------------------------------------------------
// Data Transmission Packet (DT Mode)
// -----------------------------------------------------------------------------

// DataPacket represents the structured data format for Data Transmission (DT)
type DataPacket struct {
	InterestQsf float64   // Interest QoS factor
	DataQsf     float64   // Data QoS factor
	Values      []float64 // Vector of configurable length (DataPacketValueCount)
}

// NewDataPacket creates a new DataPacket with default values
func NewDataPacket() *DataPacket {
	return &DataPacket{
		InterestQsf: 0.0,
		DataQsf:     0.0,
		Values:      make([]float64, DataPacketValueCount), // All initialized to 0.0
	}
}

// NewDataPacketWithValues creates a DataPacket with specified values
func NewDataPacketWithValues(interestQsf, dataQsf float64, values []float64) *DataPacket {
	if len(values) != DataPacketValueCount {
		// Ensure exactly DataPacketValueCount values.
		adjustedValues := make([]float64, DataPacketValueCount)
		copy(adjustedValues, values)
		values = adjustedValues
	}

	return &DataPacket{
		InterestQsf: interestQsf,
		DataQsf:     dataQsf,
		Values:      values,
	}
}

// Serialize converts DataPacket to byte array for NDN transmission
func (dp *DataPacket) SerializeData() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write InterestQsf (8 bytes)
	err := binary.Write(buf, binary.LittleEndian, dp.InterestQsf)
	if err != nil {
		return nil, fmt.Errorf("failed to write InterestQsf: %w", err)
	}

	// Write DataQsf (8 bytes)
	err = binary.Write(buf, binary.LittleEndian, dp.DataQsf)
	if err != nil {
		return nil, fmt.Errorf("failed to write DataQsf: %w", err)
	}

	// Write DataPacketValueCount double values.
	for i, value := range dp.Values {
		err = binary.Write(buf, binary.LittleEndian, value)
		if err != nil {
			return nil, fmt.Errorf("failed to write value at index %d: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// Deserialize converts byte array back to DataPacket
func DeserializeData(data []byte) (*DataPacket, error) {
	expectedSize := (dataPacketMetaFields * float64SizeBytes) + (DataPacketValueCount * float64SizeBytes)
	if len(data) != expectedSize {
		return nil, fmt.Errorf("invalid data size: expected %d bytes, got %d", expectedSize, len(data))
	}

	buf := bytes.NewReader(data)
	dp := &DataPacket{
		Values: make([]float64, DataPacketValueCount),
	}

	// Read InterestQsf
	err := binary.Read(buf, binary.LittleEndian, &dp.InterestQsf)
	if err != nil {
		return nil, fmt.Errorf("failed to read InterestQsf: %w", err)
	}

	// Read DataQsf
	err = binary.Read(buf, binary.LittleEndian, &dp.DataQsf)
	if err != nil {
		return nil, fmt.Errorf("failed to read DataQsf: %w", err)
	}

	// Read DataPacketValueCount double values.
	for i := 0; i < DataPacketValueCount; i++ {
		err = binary.Read(buf, binary.LittleEndian, &dp.Values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read value at index %d: %w", i, err)
		}
	}

	return dp, nil
}

// String returns a string representation of the DataPacket
func (dp *DataPacket) String() string {
	return fmt.Sprintf("DataPacket{InterestQsf: %.3f, DataQsf: %.3f, Values: [%d values]}",
		dp.InterestQsf, dp.DataQsf, len(dp.Values))
}

// GetSize returns the total size in bytes when serialized
func (dp *DataPacket) GetSize() int {
	return (dataPacketMetaFields * float64SizeBytes) + (len(dp.Values) * float64SizeBytes)
}

// -----------------------------------------------------------------------------
// Path Discovery Packet (PD Mode)
// -----------------------------------------------------------------------------

// DiscoveryPacket represents the structured data format for Path Discovery (PD).
// It contains no payload, only metadata flags.
type DiscoveryPacket struct {
	TierConverged bool // Metadata: Whether the tier has converged
	NodeConverged bool // Metadata: Whether the specific node has converged
}

// NewDiscoveryPacket creates a new DiscoveryPacket
func NewDiscoveryPacket(tierConv bool, nodeConv bool) *DiscoveryPacket {
	return &DiscoveryPacket{
		TierConverged: tierConv,
		NodeConverged: nodeConv,
	}
}

// SerializeDiscovery converts DiscoveryPacket to byte array
func (dp *DiscoveryPacket) SerializeDiscovery() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write Metadata: TierConverged (1 byte)
	if err := binary.Write(buf, binary.LittleEndian, dp.TierConverged); err != nil {
		return nil, fmt.Errorf("failed to write TierConverged: %w", err)
	}
	// Write Metadata: NodeConverged (1 byte)
	if err := binary.Write(buf, binary.LittleEndian, dp.NodeConverged); err != nil {
		return nil, fmt.Errorf("failed to write NodeConverged: %w", err)
	}

	return buf.Bytes(), nil
}

// DeserializeDiscovery converts byte array back to DiscoveryPacket
func DeserializeDiscovery(data []byte) (*DiscoveryPacket, error) {
	// Size = 1 (bool) + 1 (bool) = 2 bytes
	expectedSize := 1 + 1
	if len(data) != expectedSize {
		return nil, fmt.Errorf("invalid PD packet size: expected %d bytes, got %d", expectedSize, len(data))
	}

	buf := bytes.NewReader(data)
	dp := &DiscoveryPacket{}

	if err := binary.Read(buf, binary.LittleEndian, &dp.TierConverged); err != nil {
		return nil, fmt.Errorf("failed to read TierConverged: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &dp.NodeConverged); err != nil {
		return nil, fmt.Errorf("failed to read NodeConverged: %w", err)
	}

	return dp, nil
}

func (dp *DiscoveryPacket) String() string {
	return fmt.Sprintf("DiscoveryPacket(PD){TierConv: %t, NodeConv: %t}", dp.TierConverged, dp.NodeConverged)
}
