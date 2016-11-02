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

type Bytemap struct {
	bytemap *os.File
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
	err = store.Verify()

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

		//		fmt.Println("Verified", value, current, max)
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
	fmt.Println("Adding to store")
	fmt.Println("Last:", self.last)
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
	fmt.Println("Adding to store index", index, offset, length)
	self.last = index
	fmt.Println("Last:", self.last)
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

func (self *Store) FromIndexDo(index int64, handle func(*Record)) {
	for current := index + 1; current <= self.LastIndex(); current++ {
		record, err := self.GetRecord(current)
		if err != nil {
			fmt.Println("Error processing records")
			fmt.Println(err)
		} else {
			go handle(record)
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

// StoreData contains data derived from inputs
type StoreData struct {
	First int64
	Last  int64
	Data  [1500]int64
}

// Load loads freefall.StoreData from storage
func LoadStoreData(path string) (*StoreData, error) {
	var first, last int64

	file, err := os.Open("store/" + path + "/derived")
	if err != nil {
		fmt.Println("Could not open derived")
		return &StoreData{First: -1, Last: -1, Data: [1500]int64{}}, nil
	}

	buff := make([]byte, 8)
	_, err = file.Read(buff)
	if err != nil {
		return nil, err
	}

	first, _ = binary.Varint(buff)

	_, err = file.Read(buff)
	if err != nil {
		return nil, err
	}

	last, _ = binary.Varint(buff)

	data := [1500]int64{}
	for i := 0; i < 1500; i++ {
		_, err = file.Read(buff)
		if err != nil {
			return nil, err
		}

		data[i], _ = binary.Varint(buff)
	}

	file.Close()

	return &StoreData{First: first, Last: last, Data: data}, nil
}

// Save saves StoreData to storage
func (self *StoreData) Save(path string) error {
	fmt.Println("Saving...", self.Last)
	output, err := os.OpenFile("store/"+path+"/derived", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	buff := make([]byte, 8)
	binary.PutVarint(buff, self.First)
	output.Write(buff)

	binary.PutVarint(buff, self.Last)
	output.Write(buff)

	for i := 0; i < 1500; i++ {
		binary.PutVarint(buff, self.Data[i])
		output.Write(buff)
	}

	output.Close()

	return nil
}
