package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/blanu/AdversaryLab-nanomsg/freefall"

	"golang.org/x/exp/mmap"
)

func main() {
	var store *freefall.Store
	var err error

	if os.Args[1] == "verify" {
		store, err = freefall.OpenStore(os.Args[2])
		if err != nil {
			fmt.Println("Error opening store")
			fmt.Println(err)
			return
		}
		store.Close()
	} else if os.Args[1] == "add" {
		store, err = freefall.OpenStore(os.Args[2])
		if err != nil {
			fmt.Println("Error opening store")
			fmt.Println(err)
			return
		}

		value := []byte(os.Args[3])
		store.Add(value)
		store.Close()

		store, err = freefall.OpenStore(os.Args[2])
		if err != nil {
			fmt.Println("Error opening store")
			fmt.Println(err)
			return
		}
	} else if os.Args[1] == "rule" {
		bytemap, err := freefall.NewReadonlyBytemap(os.Args[2])
		if err != nil {
			fmt.Println("Error opening bytemap file", err)
			return
		}
		rule := bytemap.Extract()
		fmt.Println(len(rule))
		fmt.Println(hex.EncodeToString(rule))
	} else if os.Args[1] == "bytes" {
		bytemap, err := freefall.NewReadonlyBytemap(os.Args[2])
		if err != nil {
			fmt.Println("Error opening bytemap file", err)
			return
		}
		for i := 0; i < 256; i++ {
			index, err2 := strconv.Atoi(os.Args[3])
			if err2 != nil {
				fmt.Println("Error parsing argument", err)
				return
			}
			prev, err3 := strconv.Atoi(os.Args[4])
			if err3 != nil {
				fmt.Println("Error parsing argument", err)
				return
			}
			fmt.Print(bytemap.GetCount(int(index), byte(prev), byte(i)), " ")
		}
		fmt.Println()
	} else if os.Args[1] == "byte" {
		bytemap, err := freefall.NewReadonlyBytemap(os.Args[2])
		if err != nil {
			fmt.Println("Error opening bytemap file", err)
			return
		}
		index, err2 := strconv.Atoi(os.Args[3])
		if err2 != nil {
			fmt.Println("Error parsing argument", err2)
			return
		}
		prev, err3 := strconv.Atoi(os.Args[4])
		if err3 != nil {
			fmt.Println("Error parsing argument", err3)
			return
		}
		next, err4 := strconv.Atoi(os.Args[5])
		if err4 != nil {
			fmt.Println("Error parsing argument", err4)
			return
		}
		fmt.Println(bytemap.GetCount(int(index), byte(prev), byte(next)))
	} else if os.Args[1] == "bytemap" {
		bytemap, err := freefall.NewBytemap(os.Args[2])
		if err != nil {
			fmt.Println("Error opening bytemap file", err)
			return
		}
		store, err2 := freefall.OpenReadonlyStore(os.Args[2])
		if err2 != nil {
			fmt.Println("Error opening store")
			fmt.Println(err2)
			return
		}

		index := bytemap.GetIndex()
		fmt.Println("Processing", index, "->", store.LastIndex())
		store.BlockingFromIndexDo(index, func(record *freefall.Record) {
			bytemap.ProcessBytes(record)
		})

		store.Close()
	} else if os.Args[1] == "forcebytemap" {
		bytemap, err := freefall.NewBytemap(os.Args[2])
		if err != nil {
			fmt.Println("Error opening bytemap file", err)
			return
		}
		store, err2 := freefall.OpenReadonlyStore(os.Args[2])
		if err2 != nil {
			fmt.Println("Error opening store")
			fmt.Println(err2)
			return
		}

		store.BlockingFromIndexDo(0, func(record *freefall.Record) {
			bytemap.ForceProcessBytes(record)
		})

		store.Close()
	}
}

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

	outindex, err := os.OpenFile("store/"+path+".index", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	output, err := os.OpenFile("store/"+path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	reader, err := mmap.Open("store/" + path + ".index")
	if err != nil {
		return nil, err
	}

	file, err := os.Open("store/" + path)
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

		fmt.Println("Verified", value, current, max)
	}

	self.last = current

	fmt.Println("Verified store")
	fmt.Println(self.last)

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
	fmt.Println(self)
	fmt.Println(self.last)
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

	self.last++

	return self.last
}

func (self *Store) AddIndex(index int64, offset int64, length int64) {
	fmt.Println("Adding to store index", index, offset, length)
	fmt.Println(self.last)
	stat, err := self.outindex.Stat()
	if err != nil {
		return
	}
	ioffset := stat.Size()
	if ioffset%(8*3) != 0 {
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

func (self *Store) Close() {
	self.content.Close()
	self.outindex.Close()
	self.output.Close()
}
