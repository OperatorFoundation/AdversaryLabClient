package protocol

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/google/gopacket"
	"github.com/satori/go.uuid"
)

const (
	packetStatsKey = "Packet:Stats"

	allowedConnectionsKey = "Allowed:Connections"
	allowedIncomingKey    = "Allowed:Incoming:Packets"
	allowedOutgoingKey    = "Allowed:Outgoing:Packets"

	allowedIncomingDatesKey = "Allowed:Incoming:Dates"
	allowedOutgoingDatesKey = "Allowed:Outgoing:Dates"

	allowedPacketsSeenKey = "Allowed:Connections:Seen"

	blockedConnectionsKey = "Blocked:Connections"
	blockedIncomingKey    = "Blocked:Incoming:Packets"
	blockedOutgoingKey    = "Blocked:Outgoing:Packets"

	blockedIncomingDatesKey = "Blocked:Incoming:Dates"
	blockedOutgoingDatesKey = "Blocked:Outgoing:Dates"

	blockedPacketsSeenKey = "Blocked:Connections:Seen"

	pubsubChannel = "New:Connections:Channel"
	pubsubMessage = "NewConnectionAdded"
)

// Client holds the connection to the Redis database
type Client struct {
	conn redis.Conn
}

// ConnectionPackets holds an incoming packet and an outgoing packet
type ConnectionPackets struct {
	Incoming gopacket.Packet
	Outgoing gopacket.Packet
}

// Connect connects to the Redis database
func Connect() Client {
	conn := startRedis()

	return Client{
		conn: conn,
	}
}

func startRedis() redis.Conn {
	conn, _ := redis.Dial("tcp", "localhost:6380")

	reply, err := conn.Do("ping")
	if err == nil {
		fmt.Println("Successful ping to Redis server: ", reply)
	} else {
		fmt.Println("Redis error: ", err)
	}

	return conn
}

// AddTrainPacket adds a packet to the training data set
func (client Client) AddTrainPacket(allowBlock bool, conn ConnectionPackets) {
	connectionIDString := uuid.Must(uuid.NewV4())

	incomingPacket := conn.Incoming
	outgoingPacket := conn.Outgoing

	var incomingPayload []byte
	var outgoingPayload []byte

	var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000
	var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

	if iapp := incomingPacket.ApplicationLayer(); iapp != nil {
		incomingPayload = iapp.Payload()
	}

	if oapp := outgoingPacket.ApplicationLayer(); oapp != nil {
		outgoingPayload = oapp.Payload()
	}

	if allowBlock {
		_, _ = client.conn.Do("hset", allowedIncomingKey, connectionIDString, incomingPayload)
		_, _ = client.conn.Do("hset", allowedOutgoingKey, connectionIDString, outgoingPayload)
		_, _ = client.conn.Do("hset", allowedIncomingDatesKey, connectionIDString, incomingTime)
		_, _ = client.conn.Do("hset", allowedOutgoingDatesKey, connectionIDString, outgoingTime)
		_, _ = client.conn.Do("rpush", allowedConnectionsKey, connectionIDString)
		_, _ = client.conn.Do("hincrby", packetStatsKey, allowedPacketsSeenKey, "1")
		_, _ = client.conn.Do("publish", pubsubChannel, pubsubMessage)
	} else {
		_, _ = client.conn.Do("hset", blockedIncomingKey, connectionIDString, incomingPayload)
		_, _ = client.conn.Do("hset", blockedOutgoingKey, connectionIDString, outgoingPayload)
		_, _ = client.conn.Do("hset", blockedIncomingDatesKey, connectionIDString, incomingTime)
		_, _ = client.conn.Do("hset", blockedOutgoingDatesKey, connectionIDString, outgoingTime)
		_, _ = client.conn.Do("rpush", blockedConnectionsKey, connectionIDString)
		_, _ = client.conn.Do("hincrby", packetStatsKey, blockedPacketsSeenKey, "1")
		_, _ = client.conn.Do("publish", pubsubChannel, pubsubMessage)
	}
}

// func (client Client) AddTestPacket(dataset string, incoming bool, payload []byte) {
// 	var packet TestPacket = TestPacket{Dataset: dataset, Incoming: incoming, Payload: payload}
//
// 	var value = NamedType{Name: "protocol.TrainPacket", Value: packet}
//
// 	var buff = new(bytes.Buffer)
// 	var bw = bufio.NewWriter(buff)
// 	//  var b []byte = make([]byte, 0, 2048)
// 	var h codec.Handle = NamedTypeHandle()
//
// 	//  var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
// 	var enc *codec.Encoder = codec.NewEncoder(bw, h)
// 	var err error = enc.Encode(value)
// 	if err != nil {
// 		die("Error encoding packet: %s", err.Error())
// 	}
//
// 	bw.Flush()
//
// 	client.request(buff.Bytes())
// }

// func (client Client) GetIncomingRule(dataset string) []byte {
// 	var request RuleRequest = RuleRequest{Dataset: dataset, Incoming: true}
// 	var b []byte = make([]byte, 0, 64)
// 	var h codec.Handle = new(codec.CborHandle)
// 	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
// 	var err error = enc.Encode(request)
// 	if err != nil {
// 		return nil
// 	}
//
// 	return client.request(b)
// }

// func (client Client) GetOutgoingRule(dataset string) []byte {
// 	var request RuleRequest = RuleRequest{Dataset: dataset, Incoming: false}
// 	var b []byte = make([]byte, 0, 64)
// 	var h codec.Handle = new(codec.CborHandle)
// 	var enc *codec.Encoder = codec.NewEncoderBytes(&b, h)
// 	var err error = enc.Encode(request)
// 	if err != nil {
// 		return nil
// 	}
//
// 	return client.request(b)
// }
