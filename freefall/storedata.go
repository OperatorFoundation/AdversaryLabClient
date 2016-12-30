package freefall

import (
	"encoding/binary"
	"fmt"
	"os"
)

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
