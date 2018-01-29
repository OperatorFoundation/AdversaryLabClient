package storage

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

type LengthCounter struct {
	path  string
	conn  redis.Conn
	first bool
}

func startRedis() redis.Conn {
	conn, _ := redis.Dial("tcp", "localhost:6379")

	conn.Do("ping")

	return conn
}

func NewLengthCounter(name string) (*LengthCounter, error) {
	conn := startRedis()

	return &LengthCounter{path: name, conn: conn}, nil
}

func (self *LengthCounter) Increment(allowBlock bool, length int16) error {
	var bucket string
	if allowBlock {
		bucket = self.path + ":allow"
	} else {
		bucket = self.path + ":block"
	}

	key := strconv.Itoa(int(length))

	fullkey := bucket + ":" + "lengths"
	self.conn.Do("zincrby", fullkey, 1, key)

	return nil
}

func (self *LengthCounter) ProcessBytes(allowBlock bool, sequence []byte) {
	if !self.first {
		self.first = true
	} else {
		return
	}

	length := int16(len(sequence))

	self.Increment(allowBlock, length)

	//	self.bytemap.Save()
}
