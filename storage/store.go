package storage

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

type Record struct {
	Index int64
	Data  []byte
}

type Store struct {
	Path string
	conn redis.Conn
}

func OpenStore(path string) (*Store, error) {
	//	fmt.Println("OPEN STORE", path)

	conn := startRedis()

	store := &Store{Path: path, conn: conn}

	return store, nil
}

func (self *Store) GetRecord(index int64) (*Record, error) {
	data, err := self.conn.Do("lindex", self.Path, index)
	if err != nil {
		return nil, err
	} else {
		if data == nil {
			return nil, nil
		} else {
			return &Record{Index: index, Data: data.([]byte)}, nil
		}
	}
}

// func (self *Store) GetRecord(index int64) (*Record, error) {
// 	var offset int64
// 	var length int64
// 	var err error
// 	var bs []byte
//
// 	offset, err = self.getOffset(index)
// 	if err != nil {
// 		fmt.Println("Error in GetRecord - getOffset", index)
// 		return nil, err
// 	}
//
// 	length, err = self.getLength(index)
// 	if err != nil {
// 		fmt.Println("Error in GetRecord - getLength", index)
// 		return nil, err
// 	}
//
// 	bs = make([]byte, length)
// 	_, err = self.output.Seek(offset, os.SEEK_SET)
// 	if err != nil {
// 		fmt.Println("Error in GetRecord - Seek", offset)
// 		return nil, err
// 	}
//
// 	_, err = self.output.Read(bs)
// 	if err != nil {
// 		fmt.Println("Error in GetRecord - Read", offset)
// 		return nil, err
// 	}
//
// 	if length == 0 || len(bs) == 0 {
// 		fmt.Println("Error, zero length sequence", index, offset, length, len(bs), bs)
// 		return nil, errors.New("Error, zero length sequence")
// 	}
//
// 	return &Record{Index: index, Data: bs}, nil
// }

// func (self *Store) LastIndex() int64 {
// 	return self.last
// }

func (self *Store) Add(data []byte) int64 {
	if len(data) == 0 {
		fmt.Println("Cannot add sequence with 0 length")
		return -1
	}

	result, _ := self.conn.Do("LPUSH", self.Path, data)
	return result.(int64) - 1
}

func (self *Store) LastIndex() int64 {
	result, err := self.conn.Do("llen", self.Path)
	if err != nil {
		return -1
	} else {
		if result == nil {
			return -1
		} else {
			return result.(int64) - 1
		}
	}
}

func (self *Store) FromIndexDo(index int64, channel chan *Record) {
	// for current := index + 1; current <= self.LastIndex(); current++ {
	// 	record, err := self.GetRecord(current)
	// 	if err != nil {
	// 		fmt.Println("Error processing records")
	// 		fmt.Println(err)
	// 	} else {
	// 		channel <- record
	// 	}
	// }
}

func (self *Store) BlockingFromIndexDo(index int64, handle func(*Record)) {
	// for current := index + 1; current <= self.LastIndex(); current++ {
	// 	record, err := self.GetRecord(current)
	// 	if err != nil {
	// 		fmt.Println("Error processing records")
	// 		fmt.Println(err)
	// 	} else {
	// 		handle(record)
	// 	}
	// }
}

func (self *Store) Close() {
	self.conn.Close()
}
