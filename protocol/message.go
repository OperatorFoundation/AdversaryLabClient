package protocol

import (
	"github.com/ugorji/go/codec"
)

type TrainPacket struct {
	Dataset    string
	AllowBlock bool
	Incoming   bool
	Payload    []byte
}

type TestPacket struct {
	Dataset  string
	Incoming bool
	Payload  []byte
}

type RuleRequest struct {
	Dataset  string
	Incoming bool
}

type Rule struct {
	Dataset       string
	RequireForbid bool
	Incoming      bool
	Sequence      []byte
}

type ResultStatus int

const (
	Success ResultStatus = iota
	Error
)

func TrainPacketFromMap(data map[interface{}]interface{}) TrainPacket {
	packet := TrainPacket{}
	packet.Dataset = data["Dataset"].(string)
	packet.AllowBlock = data["AllowBlock"].(bool)
	packet.Incoming = data["Incoming"].(bool)
	packet.Payload = data["Payload"].([]byte)
	return packet
}

func TestPacketFromMap(data map[interface{}]interface{}) TestPacket {
	packet := TestPacket{}
	packet.Dataset = data["Dataset"].(string)
	packet.Incoming = data["Incoming"].(bool)
	packet.Payload = data["Payload"].([]byte)
	return packet
}

func RuleFromMap(data map[interface{}]interface{}) Rule {
	rule := Rule{}
	rule.Dataset = data["Dataset"].(string)
	rule.RequireForbid = data["RequireForbid"].(bool)
	rule.Incoming = data["Incoming"].(bool)
	rule.Sequence = data["Sequence"].([]byte)
	return rule
}

func encodeString(value string) ([]byte, error) {
	var b []byte = make([]byte, 0, 64)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var err error = enc.Encode(value)
	if err == nil {
		return nil, err
	} else {
		return b, nil
	}
}

func decodeString(b []byte) (string, error) {
	var value string
	var h codec.Handle = new(codec.CborHandle)
	var dec *codec.Decoder = codec.NewDecoderBytes(b, h)
	var err error = dec.Decode(&value)
	if err == nil {
		return "", err
	} else {
		return value, nil
	}
}

func encodeUint16(value uint16) ([]byte, error) {
	var b []byte = make([]byte, 0, 64)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var err error = enc.Encode(value)
	if err == nil {
		return nil, err
	} else {
		return b, nil
	}
}

func decodeUint16(b []byte) (uint16, error) {
	var value uint16
	var h codec.Handle = new(codec.CborHandle)
	var dec *codec.Decoder = codec.NewDecoderBytes(b, h)
	var err error = dec.Decode(&value)
	if err == nil {
		return 0, err
	} else {
		return value, nil
	}
}

func encodeUint16Slice(value []uint16) ([]byte, error) {
	var b []byte = make([]byte, 0, 64)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var err error = enc.Encode(value)
	if err == nil {
		return nil, err
	} else {
		return b, nil
	}
}

func decodeUint16Slice(b []byte) ([]uint16, error) {
	var value []uint16
	var h codec.Handle = new(codec.CborHandle)
	var dec *codec.Decoder = codec.NewDecoderBytes(b, h)
	var err error = dec.Decode(&value)
	if err == nil {
		return nil, err
	} else {
		return value, nil
	}
}
