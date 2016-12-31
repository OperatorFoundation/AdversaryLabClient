package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/ugorji/go/codec"
)

func TestInt(t *testing.T) {
	var value int = 127
	var golden []byte = []byte{24, 127}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoder(bw, h)
	var err error = enc.Encode(value)
	if err != nil {
		t.Error("Error encoding packet: " + err.Error())
	}
	bw.Flush()

	t.Log(value)
	t.Log(buff.Bytes())

	if !bytes.Equal(buff.Bytes(), golden) {
		t.Fail()
	}
}

type TestingStruct struct {
	Value int
}

func TestStruct(t *testing.T) {
	var value TestingStruct = TestingStruct{Value: 127}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoder(bw, h)
	var err error = enc.Encode(&value)
	if err != nil {
		die("Error encoding packet: %s", err.Error())
	}
	bw.Flush()

	t.Log(value)
	t.Log(buff.Bytes())
}
