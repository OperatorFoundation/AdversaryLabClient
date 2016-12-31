package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Countmap struct {
	bytemap *os.File
	Best    *RuleCandidate
	Updates chan *RuleCandidate
}

func NewCountmap(name string, updates chan *RuleCandidate) (*Countmap, error) {
	bytemap, err := os.OpenFile("store/"+name+"/countmap", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening countmap file", err)
		return nil, err
	}
	// stat, err2 := bytemap.Stat()
	// if err2 != nil {
	// 	fmt.Println("Error getting size of countmap file", err2)
	// 	return nil, err2
	// }
	// if stat.Size() == 0 {
	// 	zeros := make([]byte, int64Size)
	// 	bytemap.WriteAt(zeros, (1500*256*256+1)*int64Size)
	// }

	return &Countmap{bytemap: bytemap, Best: nil, Updates: updates}, nil
}

func (self *Countmap) IncrementCount(index int64, allowBlock bool) {
	value := self.GetCount(index, allowBlock)
	value++
	self.PutCount(index, allowBlock, value)

	self.IncrementTotal(allowBlock)

	self.keepBest(index)
}

func (self *Countmap) GetCount(index int64, allowBlock bool) int64 {
	// FIXME - check file length
	offset := self.getOffset(index, allowBlock)
	return self.getInt64(offset)
}

func (self *Countmap) PutCount(index int64, allowBlock bool, count int64) {
	offset := self.getOffset(index, allowBlock)
	self.putInt64(offset, count)
}

func (self *Countmap) IncrementTotal(allowBlock bool) {
	value := self.GetTotal(allowBlock)
	value++
	self.PutTotal(allowBlock, value)
}

func (self *Countmap) GetIndex(allowBlock bool) int64 {
	self.bytemap.Sync()
	offset := self.getHeaderOffset(indexHeaderOffset, allowBlock)
	return self.getInt64(offset)
}

func (self *Countmap) PutIndex(index int64, allowBlock bool) {
	offset := self.getHeaderOffset(indexHeaderOffset, allowBlock)
	self.putInt64(offset, index)
	self.bytemap.Sync()
}

func (self *Countmap) GetTotal(allowBlock bool) int64 {
	self.bytemap.Sync()
	offset := self.getHeaderOffset(totalHeaderOffset, allowBlock)
	return self.getInt64(offset)
}

func (self *Countmap) PutTotal(allowBlock bool, total int64) {
	offset := self.getHeaderOffset(totalHeaderOffset, allowBlock)
	self.putInt64(offset, total)
	self.bytemap.Sync()
}

func (self *Countmap) Save() {
	self.bytemap.Sync()
}

func (self *Countmap) candidate(index int64) *RuleCandidate {
	ac := self.GetCount(index, true)
	at := self.GetTotal(true)
	bc := self.GetCount(index, false)
	bt := self.GetTotal(false)

	return &RuleCandidate{Index: index, AllowCount: ac, AllowTotal: at, BlockCount: bc, BlockTotal: bt}
}

func (self *Countmap) keepBest(index int64) {
	c := self.candidate(index)
	if c.Score() == 0 {
		return
	}

	if self.Best == nil {
		self.Best = c
		if Debug {
			fmt.Println("First best rule.", self.Best, self.Best.rawScore())
		} else {
			fmt.Print("@")
		}
		self.Updates <- self.Best
	} else {
		if c.BetterThan(self.Best) {
			self.Best = c
			if Debug {
				fmt.Println("New best rule!", self.Best, self.Best.rawScore())
			} else {
				fmt.Print("*")
			}
			self.Updates <- self.Best
		}
	}
}

func (self *Countmap) getHeaderOffset(headerIndex int64, allowBlock bool) int64 {
	offset := headerIndex * cellsize
	if allowBlock {
		offset = offset + int64Size
	}

	return offset
}

func (self *Countmap) getOffset(index int64, allowBlock bool) int64 {
	offset := headerSize + (index * cellsize)
	if allowBlock {
		offset = offset + int64Size
	}

	return offset
}

func (self *Countmap) getInt64(offset int64) int64 {
	// FIXME - check file length
	buff := make([]byte, int64Size)
	self.bytemap.Seek(offset, 0)
	self.bytemap.Read(buff)
	value, _ := binary.Varint(buff)
	return value
}

func (self *Countmap) putInt64(offset int64, value int64) {
	buff := make([]byte, int64Size)
	binary.PutVarint(buff, value)
	self.bytemap.Seek(offset, 0)
	self.bytemap.Write(buff)
}
