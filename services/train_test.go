package services

import (
	"bytes"
	"testing"

	"github.com/OperatorFoundation/AdversaryLab-protocol/adversarylab"
)

func TestCountmap(t *testing.T) {
	var x int
	var buff bytes.Buffer

	t.Log("TestTrain")

	lab := adversarylab.Connect("tcp://localhost:4567")

	// Allowed incoming - 0, 01, 012, ... 0..126
	// for x = 0; x < 10; x++ {
	for x = 0; x < 127; x++ {
		buff.WriteByte(byte(x))
		lab.AddTrainPacket("testing", true, true, buff.Bytes())
	}

	// Allowed outgoing - 127, ... 127..255
	buff.Reset()
	for x = 127; x < 256; x++ {
		buff.WriteByte(byte(x))
		lab.AddTrainPacket("testing", true, false, buff.Bytes())
	}

	// Blocked incoming - 126, ... 126..0
	buff.Reset()
	for x = 126; x >= 0; x-- {
		buff.WriteByte(byte(x))
		lab.AddTrainPacket("testing", false, true, buff.Bytes())
	}

	// Blocked outgoing - 255, ... 255..127
	buff.Reset()
	for x = 255; x >= 127; x-- {
		buff.WriteByte(byte(x))
		lab.AddTrainPacket("testing", false, false, buff.Bytes())
	}
}
