package storage

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

type OffsetSequenceMap struct {
	path string
	conn redis.Conn
}

func NewOffsetSequenceMap(name string, updates chan *RuleCandidate) (*OffsetSequenceMap, error) {
	conn := startRedis()

	return &OffsetSequenceMap{path: name, conn: conn}, nil
}

func B2S(bs []uint8) string {
	ba := make([]byte, 0, len(bs))
	for _, b := range bs {
		ba = append(ba, byte(b))
	}

	return string(ba)
}

func (self *OffsetSequenceMap) Increment(allowBlock bool, offset int16, bs []byte) {
	var incr string
	var other string

	if allowBlock {
		incr = "allow"
		other = "block"
	} else {
		incr = "block"
		other = "allow"
	}

	key := self.path + ":" + incr + ":" + strconv.Itoa(int(offset))
	value, _ := self.conn.Do("zincrby", key, 1, bs)
	count, _ := self.conn.Do("zcard", key)
	var bbs []uint8
	var i int
	if value != nil {
		bbs = value.([]uint8)
		i, _ = strconv.Atoi(B2S(bbs))
	} else {
		i = 0
	}

	okey := self.path + ":" + other + ":" + strconv.Itoa(int(offset))
	ovalue, _ := self.conn.Do("zscore", okey, bs)
	ocount, _ := self.conn.Do("zcard", okey)
	var obbs []uint8
	var oi int
	if ovalue != nil {
		obbs = ovalue.([]uint8)
		oi, _ = strconv.Atoi(B2S(obbs))
	} else {
		oi = 0
	}

	var aratio, bratio float64

	if allowBlock {
		if count == 0 {
			aratio = 0
		} else {
			aratio = float64(i) / float64(count.(int64))
		}

		if ocount == 0 {
			bratio = 0
		} else {
			bratio = float64(oi) / float64(ocount.(int64))
		}
	} else {
		if count == 0 {
			bratio = 0
		} else {
			bratio = float64(i) / float64(count.(int64))
		}

		if ocount == 0 {
			aratio = 0
		} else {
			aratio = float64(oi) / float64(ocount.(int64))
		}
	}

	raa := aratio
	rbb := 1 - bratio

	faa := 1 - aratio
	fbb := bratio

	rcorrect := (raa + rbb) / 2
	fcorrect := (faa + fbb) / 2

	rkey := self.path + ":" + strconv.Itoa(int(offset)) + ":required"
	self.conn.Do("zadd", rkey, rcorrect, bs)

	fkey := self.path + ":" + strconv.Itoa(int(offset)) + ":forbidden"
	self.conn.Do("zadd", fkey, rcorrect, bs)

	masterkey := self.path + ":" + strconv.Itoa(int(offset))
	self.conn.Do("zadd", masterkey, fcorrect, bs)
	self.conn.Do("zadd", masterkey, rcorrect, bs)
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
}
