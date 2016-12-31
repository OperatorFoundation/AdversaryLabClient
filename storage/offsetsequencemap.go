package storage

import (
	"bytes"
	"encoding/binary"
)

type OffsetSequenceMap struct {
	*SequenceMap
}

func NewOffsetSequenceMap(name string, updates chan *RuleCandidate) (*OffsetSequenceMap, error) {
	result, err := NewSequenceMap(name+"-offsets", updates)
	if err != nil {
		return nil, err
	}

	return &OffsetSequenceMap{SequenceMap: result}, nil
}

func (self *OffsetSequenceMap) Increment(allowBlock bool, offset int16, bs []byte) {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.LittleEndian, offset)
	binary.Write(buff, binary.LittleEndian, bs)

	sequence := buff.Bytes()

	self.SequenceMap.Increment(allowBlock, sequence)
}

func (self *OffsetSequenceMap) ProcessBytes(allowBlock bool, sequence []byte) {
	var length int16
	var offset int16
	for length = 1; length <= int16(len(sequence)); length++ {
		// for offset = 0; offset+length <= int16(len(sequence)); offset++ {
		// 	self.Increment(allowBlock, offset, sequence[int(offset):int(offset+length)])
		// }

		offset = 0
		self.Increment(allowBlock, offset, sequence[int(offset):int(offset+length)])
	}

	self.bytemap.Save()
}
