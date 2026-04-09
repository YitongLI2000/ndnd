package datapacket

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DataPacket represents the structured data format
type DataPacket struct {
	InterestQsf float64   // Interest QoS factor
	DataQsf     float64   // Data QoS factor
	Values      []float64 // Vector of 150 double values
}

// NewDataPacket creates a new DataPacket with default values
func NewDataPacket() *DataPacket {
	return &DataPacket{
		InterestQsf: 0.0,
		DataQsf:     0.0,
		Values:      make([]float64, 150), // All initialized to 0.0
	}
}

// NewDataPacketWithValues creates a DataPacket with specified values
func NewDataPacketWithValues(interestQsf, dataQsf float64, values []float64) *DataPacket {
	if len(values) != 150 {
		// Ensure exactly 150 values
		adjustedValues := make([]float64, 150)
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
func (dp *DataPacket) Serialize() ([]byte, error) {
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

	// Write 150 double values (8 bytes each = 1200 bytes total)
	for i, value := range dp.Values {
		err = binary.Write(buf, binary.LittleEndian, value)
		if err != nil {
			return nil, fmt.Errorf("failed to write value at index %d: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// Deserialize converts byte array back to DataPacket
func Deserialize(data []byte) (*DataPacket, error) {
	expectedSize := 8 + 8 + (150 * 8) // InterestQsf + DataQsf + 150 values
	if len(data) != expectedSize {
		return nil, fmt.Errorf("invalid data size: expected %d bytes, got %d", expectedSize, len(data))
	}

	buf := bytes.NewReader(data)
	dp := &DataPacket{
		Values: make([]float64, 150),
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

	// Read 150 double values
	for i := 0; i < 150; i++ {
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
	return 8 + 8 + (len(dp.Values) * 8) // 2 metadata + values
}
