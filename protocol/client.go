package protocol

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/ugorji/go/codec"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/req"
	"github.com/go-mangos/mangos/transport/tcp"
)

type Client struct {
	sock mangos.Socket
}

func Connect(url string) Client {
	var sock mangos.Socket
	var err error

	if sock, err = req.NewSocket(); err != nil {
		die("can't get new req socket: %s", err.Error())
	}

	sock.AddTransport(tcp.NewTransport())

	if err = sock.Dial(url); err != nil {
		die("can't dial on req socket: %s", err.Error())
	}

	return Client{
		sock: sock,
	}
}

func (self Client) AddTrainPacket(dataset string, allowBlock bool, incoming bool, payload []byte) {
	var packet TrainPacket = TrainPacket{Dataset: dataset, AllowBlock: allowBlock, Incoming: incoming, Payload: payload}

	var value = NamedType{Name: "protocol.TrainPacket", Value: packet}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	//  var b []byte = make([]byte, 0, 2048)
	var h codec.Handle = NamedTypeHandle()

	//  var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var enc *codec.Encoder = codec.NewEncoder(bw, h)
	var err error = enc.Encode(value)
	if err != nil {
		die("Error encoding packet: %s", err.Error())
	}

	bw.Flush()

	self.request(buff.Bytes())
}

func (self Client) AddTestPacket(dataset string, incoming bool, payload []byte) {
	var packet TestPacket = TestPacket{Dataset: dataset, Incoming: incoming, Payload: payload}

	var value = NamedType{Name: "protocol.TrainPacket", Value: packet}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	//  var b []byte = make([]byte, 0, 2048)
	var h codec.Handle = NamedTypeHandle()

	//  var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var enc *codec.Encoder = codec.NewEncoder(bw, h)
	var err error = enc.Encode(value)
	if err != nil {
		die("Error encoding packet: %s", err.Error())
	}

	bw.Flush()

	self.request(buff.Bytes())
}

func (self Client) GetIncomingRule(dataset string) []byte {
	var request RuleRequest = RuleRequest{Dataset: dataset, Incoming: true}
	var b []byte = make([]byte, 0, 64)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var err error = enc.Encode(request)
	if err != nil {
		return nil
	}

	return self.request(b)
}

func (self Client) GetOutgoingRule(dataset string) []byte {
	var request RuleRequest = RuleRequest{Dataset: dataset, Incoming: false}
	var b []byte = make([]byte, 0, 64)
	var h codec.Handle = new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
	var err error = enc.Encode(request)
	if err != nil {
		return nil
	}

	return self.request(b)
}

func (self Client) request(data []byte) []byte {
	var err error
	var msg []byte

	fmt.Printf("AdversaryLab client sending %d\n", len(data))
	if err = self.sock.Send(data); err != nil {
		die("can't send message on push socket: %s", err.Error())
	}
	if msg, err = self.sock.Recv(); err != nil {
		die("can't receive date: %s", err.Error())
	}
	fmt.Printf("AdversaryLab client received response %s\n", string(msg))

	return msg
}
