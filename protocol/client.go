package protocol

import (
	"fmt"
	"github.com/google/gopacket"
	"gopkg.in/rethinkdb/rethinkdb-go.v5"
	"strconv"
	"time"
)

const (
	packetStatsKey = "Packet_Stats"

	allowedConnectionsKey = "Allowed_Connections"
	allowedIncomingKey    = "Allowed_Incoming_Packets"
	allowedOutgoingKey    = "Allowed_Outgoing_Packets"
	allowedIncomingDatesKey = "Allowed_Incoming_Dates"
	allowedOutgoingDatesKey = "Allowed_Outgoing_Dates"

	blockedConnectionsKey = "Blocked_Connections"
	blockedIncomingKey    = "Blocked_Incoming_Packets"
	blockedOutgoingKey    = "Blocked_Outgoing_Packets"
	blockedIncomingDatesKey = "Blocked_Incoming_Dates"
	blockedOutgoingDatesKey = "Blocked_Outgoing_Dates"

	rawPacketStatsKey = "RawPacket_Stats"

	allowedRawConnectionsKey = "Allowed_RawConnections"
	allowedIncomingNetworkPacketKey = "Allowed_Incoming_Network_Packets"
	allowedOutgoingNetworkPacketKey = "Allowed_Outgoing_Network_Packets"
	allowedIncomingTransportPacketKey = "Allowed_Incoming_Transport_Packets"
	allowedOutgoingTransportPacketKey = "Allowed_Outgoing_Transport_Packets"
	allowedRawIncomingDatesKey = "Allowed_Incoming_Dates"
	allowedRawOutgoingDatesKey = "Allowed_Outgoing_Dates"

	blockedRawConnectionsKey = "Blocked_RawConnections"
	blockedIncomingNetworkPacketKey = "Blocked_Incoming_Network_Packets"
	blockedOutgoingNetworkPacketKey = "Blocked_Outgoing_Network_Packets"
	blockedIncomingTransportPacketKey = "Blocked_Incoming_Transport_Packets"
	blockedOutgoingTransportPacketKey = "Blocked_Outgoing_Transport_Packets"
	blockedRawIncomingDatesKey = "Blocked_Incoming_Dates"
	blockedRawOutgoingDatesKey = "Blocked_Outgoing_Dates"

	allowedPacketsSeenKey = "Allowed_Connections_Seen"
	blockedPacketsSeenKey = "Blocked_Connections_Seen"

	pubsubChannel = "New_Connections_Channel"
	pubsubMessage = "NewConnectionAdded"
)

type Connection struct {
	Id    string `gorethink:"id,omitempty"`
	IncomingPayload  []byte `gorethink:"incoming_packet"`
	IncomingTimestamp int64 `gorethink:"incoming_timestamp"`
	OutgoingPayload  []byte `gorethink:"outgoing_packet,omitempty"`
	OutgoingTimestamp int64 `gorethink:"outgoing_timestamp,omitempty"`
}

type RawPacket struct {
	Connection    string `gorethink:"connection"`
	IPPacket  []byte `gorethink:"ip_packet"`
	TCPPacket  []byte `gorethink:"tcp_packet"`
	Timestamp int64 `gorethink:"timestamp"`
}

// Client holds the connection to the Redis database
type Client struct {
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
func Connect() (*Client, error) {
	session, sessionErr := startRethink()
	if sessionErr != nil {
		return nil, sessionErr
	}

	fmt.Println("Rethink session!")

	return &Client{
		session: session,
	}, nil
}

func startRethink() (*rethinkdb.Session, error) {
	fmt.Println("init rethink")
	url := "localhost:28015"
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Address: url, // endpoint without http
	})
	if err != nil {
		return nil, err
	}

	_, createErr := rethinkdb.DBCreate("AdversaryLab").RunWrite(session)
	if createErr != nil {
		fmt.Println("Could not create database", createErr)
	}

	return session, nil
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

	_, tableCreateErr := rethinkdb.DB("AdversaryLab").TableCreate(allowedIncomingNetworkPacketKey).RunWrite(client.session)
	if tableCreateErr != nil {
		fmt.Println("Could not create table", tableCreateErr)
	}
	_, tableCreateErr = rethinkdb.DB("AdversaryLab").TableCreate(allowedOutgoingNetworkPacketKey).RunWrite(client.session)
	if tableCreateErr != nil {
		fmt.Println("Could not create table", tableCreateErr)
	}
	_, tableCreateErr = rethinkdb.DB("AdversaryLab").TableCreate(blockedIncomingNetworkPacketKey).RunWrite(client.session)
	if tableCreateErr != nil {
		fmt.Println("Could not create table", tableCreateErr)
	}
	_, tableCreateErr = rethinkdb.DB("AdversaryLab").TableCreate(blockedOutgoingNetworkPacketKey).RunWrite(client.session)
	if tableCreateErr != nil {
		fmt.Println("Could not create table", tableCreateErr)
	}

	for _, incomingPacket := range conn.Incoming {

		var incomingNetworkLayerContents []byte
		var incomingTransportLayerContents []byte
		var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		fmt.Println(connectionIDString)
		fmt.Print(incomingNetworkLayerContents)
		fmt.Println(incomingTransportLayerContents)
		fmt.Println(incomingTime)

		if incomingNetworkLayer := incomingPacket.NetworkLayer(); incomingNetworkLayer != nil {
			incomingNetworkLayerContents = incomingNetworkLayer.LayerContents()
		}

		if incomingTransportLayer := incomingPacket.TransportLayer(); incomingTransportLayer != nil {
			incomingTransportLayerContents = incomingTransportLayer.LayerContents()
		}

		connValue := RawPacket{
			Connection: connectionIDString,
			IPPacket:   incomingNetworkLayerContents,
			TCPPacket:  incomingTransportLayerContents,
			Timestamp:  incomingTime,
		}

		if allowBlock {
			rethinkdb.DB("AdversaryLab").Table(allowedIncomingNetworkPacketKey).Insert(connValue).RunWrite(client.session)
		} else {
			rethinkdb.DB("AdversaryLab").Table(blockedIncomingNetworkPacketKey).Insert(connValue).RunWrite(client.session)
		}
	}

	// If  there is an outgoing packet, be sure to save that packet and its timestamp too.
	for  _, outgoingPacket := range conn.Outgoing {
		var outgoingNetworkLayerContents []byte
		var outgoingTransportLayerContents []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		fmt.Println(outgoingNetworkLayerContents)
		fmt.Println(outgoingTransportLayerContents)
		fmt.Println(outgoingTime)

		if outgoingNetworkLayer := outgoingPacket.NetworkLayer(); outgoingNetworkLayer != nil {
			outgoingNetworkLayerContents = outgoingNetworkLayer.LayerContents()
		}

		if outgoingTransportLayer := outgoingPacket.TransportLayer(); outgoingTransportLayer != nil {
			outgoingTransportLayerContents = outgoingTransportLayer.LayerContents()
		}

		connValue := RawPacket{
			Connection: connectionIDString,
			IPPacket:   outgoingNetworkLayerContents,
			TCPPacket:  outgoingTransportLayerContents,
			Timestamp:  outgoingTime,
		}

		if allowBlock {
			rethinkdb.DB("AdversaryLab").Table(allowedOutgoingNetworkPacketKey).Insert(connValue).RunWrite(client.session)
		} else {
			rethinkdb.DB("AdversaryLab").Table(blockedOutgoingNetworkPacketKey).Insert(connValue).RunWrite(client.session)
		}
	}
}

// AddTrainPacket adds a packet to the training data set
func (client Client) AddTrainPacket(allowBlock bool, conn ConnectionPackets) {
	var connValue Connection

	connectionIDString := makeConnectionID()
	incomingPacket := conn.Incoming

	var incomingPayload []byte
	var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

	fmt.Println(incomingPayload)
	fmt.Println(incomingTime)

	if iapp := incomingPacket.ApplicationLayer(); iapp != nil {
		incomingPayload = iapp.Payload()
	}

	var key string

	if allowBlock {
		key = allowedConnectionsKey
	} else {
		key = blockedConnectionsKey
	}

	_, tableCreateErr := rethinkdb.DB("AdversaryLab").TableCreate(key).RunWrite(client.session)
	if tableCreateErr != nil {
		fmt.Println("Could not create table", tableCreateErr)
	}

	// In some cases we will get a conn that only has an incoming packet
	// This should only ever happen if the conn is blocked
	// Here we save our incoming packet and connection ID
	if  conn.Outgoing != nil {
		outgoingPacket := conn.Outgoing

		var outgoingPayload []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		fmt.Println(outgoingPayload)
		fmt.Println(outgoingTime)

		if oapp := outgoingPacket.ApplicationLayer(); oapp != nil {
			outgoingPayload = oapp.Payload()
		}

		connValue = Connection{Id: connectionIDString, IncomingPayload: incomingPayload, IncomingTimestamp: incomingTime, OutgoingPayload: outgoingPayload, OutgoingTimestamp: outgoingTime}
	} else {
		connValue = Connection{Id: connectionIDString, IncomingPayload: incomingPayload, IncomingTimestamp: incomingTime, OutgoingPayload: nil, OutgoingTimestamp: 0}
	}

	rethinkdb.DB("AdversaryLab").Table(key).Insert(connValue).RunWrite(client.session)
}
