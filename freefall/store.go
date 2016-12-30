package freefall

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Record struct {
	Index int64
	Data  []byte
}

type Store struct {
	Path                   string
	outindex               *os.File
	output                 *os.File
	last                   int64
	expectedOutputLength   int64
	expectedOutindexLength int64
}

func OpenStore(path string) (*Store, error) {
	//	fmt.Println("OPEN STORE", path)
	os.Mkdir("store", 0777)
	os.Mkdir("store/"+path, 0777)

	outindex, err := os.OpenFile("store/"+path+"/index", os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}
	eoil, err := outindex.Seek(0, os.SEEK_END) // End of file
	if err != nil {
		return nil, err
	}

	output, err2 := os.OpenFile("store/"+path+"/source", os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err2 != nil {
		return nil, err2
	}
	eol, err3 := output.Seek(0, os.SEEK_END) // End of file
	if err3 != nil {
		fmt.Println("output seek failed", err3)
		return nil, err3
	}

	store := &Store{Path: path, outindex: outindex, output: output, last: -1, expectedOutputLength: eol, expectedOutindexLength: eoil}
	//	fmt.Println("verifying", path)
	// FIXME - fix the problems that cause verification to fail
	err = store.Verify()
	if err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func (self *Store) Verify() error {
	imax, err := self.outindex.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	max := int64(imax) / storeCellByteSize
	if max == 0 {
		return nil
	}

	var current int64
	for current = 0; current < max; current++ {
		value, err := self.getIndex(current)
		if err != nil {
			return err
		}

		if value != current {
			fmt.Println("invalid: found", value, "expected:", current, max)
			return errors.New("...Store verification failed: Invalid index " + string(value) + " " + string(current))
		}

		//fmt.Println("Verified", value, current, max)
		self.last = current
	}

	//	fmt.Println("Verified store")
	//	fmt.Println("Last:", self.last)

	return nil
}

func (self *Store) getIndex(index int64) (int64, error) {
	return self.getInt64((index * storeCellByteSize) + indexStoreCellOffset)
}

func (self *Store) getOffset(index int64) (int64, error) {
	return self.getInt64((index * storeCellByteSize) + offsetStoreCellOffset)
}

func (self *Store) getLength(index int64) (int64, error) {
	return self.getInt64((index * storeCellByteSize) + lengthStoreCellOffset)
}

func (self *Store) getInt64(index int64) (int64, error) {
	bs := make([]byte, int64Size)
	_, err := self.outindex.Seek(index, io.SeekStart)
	if err != nil {
		fmt.Println("Error in getInt64 Seek", err)
		return -1, err
	}
	_, err2 := self.outindex.Read(bs)
	if err2 != nil {
		fmt.Println("Error in getInt64 Read", self.Path, index)
		return -1, err2
	}
	value, _ := binary.Varint(bs)

	if Debug {
		fmt.Println("getInt64", index, len(bs), bs)
	}

	return value, nil
}

func (self *Store) GetRecord(index int64) (*Record, error) {
	var offset int64
	var length int64
	var err error
	var bs []byte

	offset, err = self.getOffset(index)
	if err != nil {
		fmt.Println("Error in GetRecord - getOffset", index)
		return nil, err
	}

	length, err = self.getLength(index)
	if err != nil {
		fmt.Println("Error in GetRecord - getLength", index)
		return nil, err
	}

	bs = make([]byte, length)
	_, err = self.output.Seek(offset, os.SEEK_SET)
	if err != nil {
		fmt.Println("Error in GetRecord - Seek", offset)
		return nil, err
	}

	_, err = self.output.Read(bs)
	if err != nil {
		fmt.Println("Error in GetRecord - Read", offset)
		return nil, err
	}

	if length == 0 || len(bs) == 0 {
		fmt.Println("Error, zero length sequence", index, offset, length, len(bs), bs)
		return nil, errors.New("Error, zero length sequence")
	}

	return &Record{Index: index, Data: bs}, nil
}

func (self *Store) LastIndex() int64 {
	return self.last
}

func (self *Store) Add(data []byte) int64 {
	if len(data) == 0 {
		fmt.Println("Cannot add sequence with 0 length")
		return -1
	}

	if Debug {
		fmt.Println("Adding to store", self, self.Path, self.last)
	}

	index := self.last + 1

	// stat, err := self.output.Stat()
	// if err != nil {
	// 	return -1
	// }
	// offset := stat.Size()

	length := int64(len(data))
	offset := self.expectedOutputLength

	//	fmt.Println("Adding", offset, length, offset+length)

	self.output.Write(data)
	self.output.Sync()
	self.expectedOutputLength = self.expectedOutputLength + length

	self.AddIndex(index, offset, length)

	return self.last
}

func (self *Store) AddIndex(index int64, offset int64, length int64) {
	if length == 0 {
		fmt.Println("Cannot add sequence with 0 length")
		return
	}

	if Debug {
		fmt.Println("Adding to store index", index, offset, length, self.last)
	}
	self.last = index
	//	fmt.Println("Last:", self.last)

	ioffset := self.expectedOutindexLength

	if ioffset%(storeCellByteSize) != 0 {
		// FIXME - reduce index and last
		roundedSize := (ioffset / storeCellByteSize) * storeCellByteSize
		fmt.Println("Truncating index", ioffset, storeCellByteSize, ioffset%storeCellByteSize, roundedSize)
		self.outindex.Truncate(roundedSize)
	}

	data := make([]byte, int64Size)

	binary.PutVarint(data, index)
	self.outindex.Write(data)

	binary.PutVarint(data, offset)
	self.outindex.Write(data)

	binary.PutVarint(data, length)
	self.outindex.Write(data)

	self.outindex.Sync()

	self.expectedOutindexLength = self.expectedOutindexLength + storeCellByteSize
}

func (self *Store) FromIndexDo(index int64, channel chan *Record) {
	for current := index + 1; current <= self.LastIndex(); current++ {
		record, err := self.GetRecord(current)
		if err != nil {
			fmt.Println("Error processing records")
			fmt.Println(err)
		} else {
			channel <- record
		}
	}
}

func (self *Store) BlockingFromIndexDo(index int64, handle func(*Record)) {
	for current := index + 1; current <= self.LastIndex(); current++ {
		record, err := self.GetRecord(current)
		if err != nil {
			fmt.Println("Error processing records")
			fmt.Println(err)
		} else {
			handle(record)
		}
	}
}

func (self *Store) Close() {
	self.outindex.Close()
	self.output.Close()
}
