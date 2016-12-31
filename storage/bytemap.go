package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type Bytemap struct {
	bytemap *os.File
}

func NewReadonlyBytemap(name string) (*Bytemap, error) {
	bytemap, err := os.OpenFile("store/"+name+"/bytemap", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("Error opening bytemap file", err)
		return nil, err
	}

	return &Bytemap{bytemap: bytemap}, nil
}

func NewBytemap(name string) (*Bytemap, error) {
	bytemap, err := os.OpenFile("store/"+name+"/bytemap", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening bytemap file", err)
		return nil, err
	}
	stat, err2 := bytemap.Stat()
	if err2 != nil {
		fmt.Println("Error getting size of bytemap file", err2)
		return nil, err2
	}
	if stat.Size() == 0 {
		zeros := make([]byte, 8)
		bytemap.WriteAt(zeros, (1500*256*256+1)*8)
	}

	return &Bytemap{bytemap: bytemap}, nil
}

func (self *Bytemap) IncrementCount(index int, prev byte, current byte) {
	value := self.GetCount(index, prev, current)
	value++
	self.PutCount(index, prev, current, value)
}

func (self *Bytemap) GetCount(index int, prev byte, current byte) int64 {
	buff := make([]byte, 8)
	offset := self.getOffset(index, prev, current)
	self.bytemap.Seek(offset, 0)
	self.bytemap.Read(buff)
	value, _ := binary.Varint(buff)
	return value
}

func (self *Bytemap) PutCount(index int, prev byte, current byte, count int64) {
	buff := make([]byte, 8)
	offset := self.getOffset(index, prev, current)
	binary.PutVarint(buff, count)
	self.bytemap.Seek(offset, 0)
	self.bytemap.Write(buff)
}

func (self *Bytemap) GetIndex() int64 {
	self.bytemap.Sync()
	buff := make([]byte, 8)
	self.bytemap.Seek(0, 0)
	_, err := self.bytemap.ReadAt(buff, 0)
	if err != nil {
		fmt.Println("Error reading", err, self.bytemap)
	}
	value, _ := binary.Varint(buff)
	return value
}

func (self *Bytemap) PutIndex(index int64) {
	buff := make([]byte, 8)
	binary.PutVarint(buff, index)
	self.bytemap.Seek(0, 0)
	_, err := self.bytemap.Write(buff)
	if err != nil {
		fmt.Println("Error writing", err)
	}
	self.bytemap.Sync()
}

func (self *Bytemap) getOffset(index int, prev byte, current byte) int64 {
	cellsize := int64(8)
	rowsize := int64(256 * 8)
	blocksize := int64(256 * 256 * 8)
	return (blocksize * int64(index)) + (rowsize * int64(prev)) + (cellsize * int64(current)) + 8
}

func (self *Bytemap) GetMax(index int, prev byte) (resultFound bool, resultIndex byte, count int64) {
	var found bool
	var maxIndex byte
	var maxCount int64

	found = false
	maxIndex = 0
	maxCount = 0

	for currentIndex := 0; currentIndex < 256; currentIndex++ {
		currentCount := self.GetCount(index, prev, byte(currentIndex))
		if (currentCount > 0) && (currentCount > maxCount) {
			found = true
			maxIndex = byte(currentIndex)
			maxCount = currentCount
		}
	}

	return found, maxIndex, maxCount
}

func (self *Bytemap) GetTotal(index int, prev byte) int64 {
	var total int64

	total = 0

	for currentIndex := 0; currentIndex < 256; currentIndex++ {
		currentCount := self.GetCount(index, prev, byte(currentIndex))
		total = total + currentCount
	}

	return total
}

func (self *Bytemap) Extract() []byte {
	var buff bytes.Buffer

	wbuff := make([]byte, 1)

	var prev byte
	prev = 0
	for index := 0; index < 1500; index++ {
		total := self.GetTotal(index, prev)
		foundMax, next, count := self.GetMax(index, prev)
		if foundMax {
			freq := (count * 100) / total
			if freq > 50 {
				fmt.Println("Extracting", index, prev, next, freq)
				wbuff[0] = next
				buff.Write(wbuff)
				prev = next
			} else {
				break
			}
		} else {
			break
		}
	}

	return buff.Bytes()
}

func (self *Bytemap) ProcessBytes(record *Record) {
	index := self.GetIndex()
	if record.Index != index+1 {
		fmt.Println("Rejecting record", record.Index, "should be", index+1)
	}
	var prev byte
	prev = 0
	for i, value := range record.Data {
		self.IncrementCount(i, prev, value)
		prev = value
	}
	self.PutIndex(record.Index)
	self.bytemap.Sync()
}

func (self *Bytemap) ForceProcessBytes(record *Record) {
	var prev byte
	prev = 0
	for i, value := range record.Data {
		self.IncrementCount(i, prev, value)
		prev = value
	}
	self.PutIndex(record.Index)
	self.bytemap.Sync()
}

func (self *Bytemap) Save() {
	self.bytemap.Sync()
}
