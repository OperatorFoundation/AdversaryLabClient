package protocol

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/google/gopacket"
	"gopkg.in/rethinkdb/rethinkdb-go.v5"
	"log"
	"strconv"
	"time"
)

const (
	packetStatsKey = "Packet:Stats"

	allowedConnectionsKey = "Allowed:Connections"
	allowedIncomingKey    = "Allowed:Incoming:Packets"
	allowedOutgoingKey    = "Allowed:Outgoing:Packets"
	allowedIncomingDatesKey = "Allowed:Incoming:Dates"
	allowedOutgoingDatesKey = "Allowed:Outgoing:Dates"

	blockedConnectionsKey = "Blocked:Connections"
	blockedIncomingKey    = "Blocked:Incoming:Packets"
	blockedOutgoingKey    = "Blocked:Outgoing:Packets"
	blockedIncomingDatesKey = "Blocked:Incoming:Dates"
	blockedOutgoingDatesKey = "Blocked:Outgoing:Dates"

	rawPacketStatsKey = "RawPacket:Stats"

	allowedRawConnectionsKey = "Allowed:RawConnections"
	allowedIncomingNetworkPacketKey = "Allowed:Incoming:Network:Packets"
	allowedOutgoingNetworkPacketKey = "Allowed:Outgoing:Network:Packets"
	allowedIncomingTransportPacketKey = "Allowed:Incoming:Transport:Packets"
	allowedOutgoingTransportPacketKey = "Allowed:Outgoing:Transport:Packets"
	allowedRawIncomingDatesKey = "Allowed:Incoming:Dates"
	allowedRawOutgoingDatesKey = "Allowed:Outgoing:Dates"

	blockedRawConnectionsKey = "Blocked:RawConnections"
	blockedIncomingNetworkPacketKey = "Blocked:Incoming:Network:Packets"
	blockedOutgoingNetworkPacketKey = "Blocked:Outgoing:Network:Packets"
	blockedIncomingTransportPacketKey = "Blocked:Incoming:Transport:Packets"
	blockedOutgoingTransportPacketKey = "Blocked:Outgoing:Transport:Packets"
	blockedRawIncomingDatesKey = "Blocked:Incoming:Dates"
	blockedRawOutgoingDatesKey = "Blocked:Outgoing:Dates"

	allowedPacketsSeenKey = "Allowed:Connections:Seen"
	blockedPacketsSeenKey = "Blocked:Connections:Seen"

	pubsubChannel = "New:Connections:Channel"
	pubsubMessage = "NewConnectionAdded"
)

// Client holds the connection to the Redis database
type Client struct {
	conn redis.Conn
	session *rethinkdb.Session
}

// ConnectionPackets holds an incoming packet and an outgoing packet
type ConnectionPackets struct {
	Incoming gopacket.Packet
	Outgoing gopacket.Packet
}

// RawConnectionPackets holds a slice of incoming packets and a slice of outgoing packet
type RawConnectionPackets struct {
	Incoming []gopacket.Packet
	Outgoing []gopacket.Packet
}

// Connect connects to the Redis database
func Connect() Client {
	conn := startRedis()
	session := startRethink()

	return Client{
		conn: conn,
		session: session,
	}
}

func startRedis() redis.Conn {
	conn, _ := redis.Dial("tcp", "localhost:6380")

	reply, err := conn.Do("ping")
	if err == nil {
		fmt.Println("-> Successful ping to Redis server: ", reply)
	} else {
		fmt.Println("-> Redis error: ", err)
	}

	return conn
}

func startRethink() *rethinkdb.Session {
	url := "localhost:28015"
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Address: url, // endpoint without http
	})
	if err != nil {
		log.Fatalln(err)
	}

	return session
}

func makeConnectionID() string {
	currentTime := time.Now()
	timeNumber := currentTime.UnixNano()
	connectionID := strconv.Itoa(int(timeNumber))
	return connectionID
}


// AddRawTrainPacket adds a complete raw packet to the training data set.
// Eventually we will use this in lieu of AddTrainPacket, currently we use both.
func (client Client) AddRawTrainPacket(allowBlock bool, conn RawConnectionPackets) {

	connectionIDString := makeConnectionID()

	for _, incomingPacket := range conn.Incoming {

		var incomingNetworkLayerContents []byte
		var incomingTransportLayerContents []byte
		var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		if incomingNetworkLayer := incomingPacket.NetworkLayer(); incomingNetworkLayer != nil {
			incomingNetworkLayerContents = incomingNetworkLayer.LayerContents()
		}

		if incomingTransportLayer := incomingPacket.TransportLayer(); incomingTransportLayer != nil {
			incomingTransportLayerContents = incomingTransportLayer.LayerContents()
		}

		if allowBlock {
			_, _ = client.conn.Do("hset", allowedIncomingNetworkPacketKey, connectionIDString, incomingNetworkLayerContents)
			_, _ = client.conn.Do("hset", allowedIncomingTransportPacketKey, connectionIDString, incomingTransportLayerContents)
			_, _ = client.conn.Do("hset", allowedRawIncomingDatesKey, connectionIDString, incomingTime)
			_, _ = client.conn.Do("rpush", allowedRawConnectionsKey, connectionIDString)
			_, _ = client.conn.Do("hincrby", rawPacketStatsKey, allowedPacketsSeenKey, "1")
		} else {
			_, _ = client.conn.Do("hset", blockedIncomingNetworkPacketKey, connectionIDString, incomingNetworkLayerContents)
			_, _ = client.conn.Do("hset", blockedIncomingTransportPacketKey, connectionIDString, incomingTransportLayerContents)
			_, _ = client.conn.Do("hset", blockedRawIncomingDatesKey, connectionIDString, incomingTime)
			_, _ = client.conn.Do("rpush", blockedRawConnectionsKey, connectionIDString)
			_, _ = client.conn.Do("hincrby", rawPacketStatsKey, blockedPacketsSeenKey, "1")
		}
	}

	// If  there is an outgoing packet, be sure to save that packet and its timestamp too.
	for  _, outgoingPacket := range conn.Outgoing {
		var outgoingNetworkLayerContents []byte
		var outgoingTransportLayerContents []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		if outgoingNetworkLayer := outgoingPacket.NetworkLayer(); outgoingNetworkLayer != nil {
			outgoingNetworkLayerContents = outgoingNetworkLayer.LayerContents()
		}

		if outgoingTransportLayer := outgoingPacket.TransportLayer(); outgoingTransportLayer != nil {
			outgoingTransportLayerContents = outgoingTransportLayer.LayerContents()
		}

		if allowBlock {
			_, _ = client.conn.Do("hset", allowedOutgoingNetworkPacketKey, connectionIDString, outgoingNetworkLayerContents)
			_, _ = client.conn.Do("hset", allowedOutgoingTransportPacketKey, connectionIDString, outgoingTransportLayerContents)
			_, _ = client.conn.Do("hset", allowedRawOutgoingDatesKey, connectionIDString, outgoingTime)
		} else {
			_, _ = client.conn.Do("hset", blockedOutgoingNetworkPacketKey, connectionIDString, outgoingNetworkLayerContents)
			_, _ = client.conn.Do("hset", blockedOutgoingTransportPacketKey, connectionIDString, outgoingTransportLayerContents)
			_, _ = client.conn.Do("hset", blockedRawOutgoingDatesKey, connectionIDString, outgoingTime)
		}
	}

	// Now we can let Adversary Lab know that there is connection data to analyze.
	_, _ = client.conn.Do("publish", pubsubChannel, pubsubMessage)
}

// AddTrainPacket adds a packet to the training data set
func (client Client) AddTrainPacket(allowBlock bool, conn ConnectionPackets) {

	connectionIDString := makeConnectionID()
	incomingPacket := conn.Incoming

	var incomingPayload []byte
	var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

	if iapp := incomingPacket.ApplicationLayer(); iapp != nil {
		incomingPayload = iapp.Payload()
	}

	// In some cases we will get a conn that only has an incoming packet
	// This should only ever happen if the conn is blocked
	// Here we save our incoming packet and connection ID
	if allowBlock {
		_, _ = client.conn.Do("hset", allowedIncomingKey, connectionIDString, incomingPayload)
		_, _ = client.conn.Do("hset", allowedIncomingDatesKey, connectionIDString, incomingTime)
		_, _ = client.conn.Do("rpush", allowedConnectionsKey, connectionIDString)
		_, _ = client.conn.Do("hincrby", packetStatsKey, allowedPacketsSeenKey, "1")
	} else {
		_, _ = client.conn.Do("hset", blockedIncomingKey, connectionIDString, incomingPayload)
		_, _ = client.conn.Do("hset", blockedIncomingDatesKey, connectionIDString, incomingTime)
		_, _ = client.conn.Do("rpush", blockedConnectionsKey, connectionIDString)
		_, _ = client.conn.Do("hincrby", packetStatsKey, blockedPacketsSeenKey, "1")
	}

	// If  there is an outgoing packet, be sure to save that packet and its timestamp too.
	if  conn.Outgoing != nil {
		outgoingPacket := conn.Outgoing

		var outgoingPayload []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		if oapp := outgoingPacket.ApplicationLayer(); oapp != nil {
			outgoingPayload = oapp.Payload()
		}

		if allowBlock {
			_, _ = client.conn.Do("hset", allowedOutgoingKey, connectionIDString, outgoingPayload)
			_, _ = client.conn.Do("hset", allowedOutgoingDatesKey, connectionIDString, outgoingTime)
		} else {
			_, _ = client.conn.Do("hset", blockedOutgoingKey, connectionIDString, outgoingPayload)
			_, _ = client.conn.Do("hset", blockedOutgoingDatesKey, connectionIDString, outgoingTime)
		}
	}

	// Now we can let Adversary Lab know that there is connection data to analyze.
	_, _ = client.conn.Do("publish", pubsubChannel, pubsubMessage)
}
