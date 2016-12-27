package freefall

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"golang.org/x/exp/mmap"
)

type Record struct {
	Index int64
	Data  []byte
}

type Store struct {
	index    *mmap.ReaderAt
	content  *os.File
	outindex *os.File
	output   *os.File
	last     int64
}

func OpenStore(path string) (*Store, error) {
	os.Mkdir("store", 0777)
	os.Mkdir("store/"+path, 0777)

	outindex, err := os.OpenFile("store/"+path+"/index", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	output, err := os.OpenFile("store/"+path+"/source", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	reader, err := mmap.Open("store/" + path + "/index")
	if err != nil {
		return nil, err
	}

	file, err := os.Open("store/" + path + "/source")
	if err != nil {
		return nil, err
	}

	store := &Store{index: reader, content: file, outindex: outindex, output: output, last: -1}
	fmt.Println("verifying", path)
	// FIXME - fix the problems that cause verification to fail
	//	err = store.Verify()
	err = nil

	if err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func OpenReadonlyStore(path string) (*Store, error) {
	reader, err := mmap.Open("store/" + path + "/index")
	if err != nil {
		return nil, err
	}

	file, err := os.Open("store/" + path + "/source")
	if err != nil {
		return nil, err
	}

	store := &Store{index: reader, content: file, outindex: nil, output: nil, last: -1}
	err = store.Verify()

	if err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func (self *Store) Verify() error {
	var max int64 = int64(self.index.Len()) / (8 * 3)

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
			fmt.Println("invalid", value, current, max)
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
	return self.getInt64(index * 8 * 3)
}

func (self *Store) getOffset(index int64) (int64, error) {
	return self.getInt64((index * 8 * 3) + 8)
}

func (self *Store) getLength(index int64) (int64, error) {
	return self.getInt64((index * 8 * 3) + (8 * 2))
}

func (self *Store) getInt64(index int64) (int64, error) {
	bs := make([]byte, 8)
	self.index.ReadAt(bs, index)
	value, _ := binary.Varint(bs)

	return value, nil
}

func (self *Store) GetRecord(index int64) (*Record, error) {
	var offset int64
	var length int64
	var err error
	var bs []byte

	offset, err = self.getOffset(index)
	if err != nil {
		return nil, err
	}

	length, err = self.getLength(index)
	if err != nil {
		return nil, err
	}

	bs = make([]byte, length)
	_, err = self.content.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	_, err = self.content.Read(bs)
	if err != nil {
		return nil, err
	}

	return &Record{Index: index, Data: bs}, nil
}

func (self *Store) LastIndex() int64 {
	return self.last
}

func (self *Store) Add(data []byte) int64 {
	//	fmt.Println("Adding to store")
	//	fmt.Println("Last:", self.last)
	index := self.last + 1
	stat, err := self.output.Stat()
	if err != nil {
		return -1
	}
	offset := stat.Size()

	length := int64(len(data))

	self.output.Seek(0, 1) // End of file
	self.output.Write(data)
	self.output.Sync()

	self.AddIndex(index, offset, length)

	return self.last
}

func (self *Store) AddIndex(index int64, offset int64, length int64) {
	//	fmt.Println("Adding to store index", index, offset, length)
	self.last = index
	//	fmt.Println("Last:", self.last)
	stat, err := self.outindex.Stat()
	if err != nil {
		return
	}
	ioffset := stat.Size()
	if ioffset%(8*3) != 0 {
		// FIXME - reduce index and last
		roundedSize := (ioffset / (8 * 3)) * (8 * 3)
		fmt.Println("Truncating index", ioffset, roundedSize)
		self.outindex.Truncate(roundedSize)
	}

	elems := []int64{index, offset, length}
	bss := make([][]byte, 3)
	for i := 0; i < len(elems); i++ {
		bss[i] = make([]byte, 8)
		binary.PutVarint(bss[i], elems[i])
	}

	sep := make([]byte, 0)
	data := bytes.Join(bss, sep)

	self.outindex.Seek(0, 1) // End of file
	self.outindex.Write(data)
	self.outindex.Sync()
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
	self.content.Close()
	self.outindex.Close()
	self.output.Close()
}

// StoreData contains data derived from inputs
type StoreData struct {
	Last int64
}

// Load loads freefall.StoreData from storage
func LoadStoreData(path string) (*StoreData, error) {
	var last int64

	file, err := os.Open("store/" + path + "/derived")
	if err != nil {
		fmt.Println("Could not open derived")
		return &StoreData{Last: -1}, nil
	}

	buff := make([]byte, 8)
	_, err = file.Read(buff)
	if err != nil {
		return nil, err
	}

	last, _ = binary.Varint(buff)

	file.Close()

	return &StoreData{Last: last}, nil
}

// Save saves StoreData to storage
func (self *StoreData) Save(path string) error {
	fmt.Println("Saving...", self.Last)
	output, err := os.OpenFile("store/"+path+"/derived", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	buff := make([]byte, 8)
	binary.PutVarint(buff, self.Last)
	output.Write(buff)

	output.Close()

	return nil
}
