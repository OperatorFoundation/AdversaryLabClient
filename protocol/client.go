package protocol

import (
	"fmt"
	"github.com/google/gopacket"
	"gopkg.in/rethinkdb/rethinkdb-go.v5"
	"strconv"
	"time"
)

const (
	packetsKey = "Packets"
)

type RawPacket struct {
	Connection    string `gorethink:"connection"`
	IPPacket  []byte `gorethink:"ip_packet"`
	TCPPacket  []byte `gorethink:"tcp_packet"`
	Payload []byte `gorethink:"payload"`
	Timestamp int64 `gorethink:"timestamp"`
	AllowBlock bool `gorethink:"allow_block"`
	InOut bool `gorethink:"in_out"`
	Handshake bool `gorethink:"handshake"`
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

	fmt.Println("-> Rethink session!")

	return &Client{
		session: session,
	}, nil
}

func startRethink() (*rethinkdb.Session, error) {
	fmt.Println("-> init rethink")
	url := "localhost:28015"
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Address: url, // endpoint without http
	})
	if err != nil {
		return nil, err
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
func (client Client) AddRawTrainPacket(transport string, allowBlock bool, conn RawConnectionPackets) {
	connectionIDString := makeConnectionID()

	_, createErr := rethinkdb.DBCreate(transport).RunWrite(client.session)
	if createErr == nil {
		fmt.Println("-> Created database", transport)
	}

	_, tableCreateErr := rethinkdb.DB(transport).TableCreate(packetsKey).RunWrite(client.session)
	if tableCreateErr == nil {
		fmt.Println("-> Created table", transport, packetsKey)
	}

	for _, incomingPacket := range conn.Incoming {

		var incomingNetworkLayerContents []byte
		var incomingTransportLayerContents []byte
		var incomingPayload []byte
		var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		//fmt.Println(connectionIDString)
		//fmt.Print(incomingNetworkLayerContents)
		//fmt.Println(incomingTransportLayerContents)
		//fmt.Println(incomingTime)

		if incomingNetworkLayer := incomingPacket.NetworkLayer(); incomingNetworkLayer != nil {
			incomingNetworkLayerContents = incomingNetworkLayer.LayerContents()
		}

		if incomingTransportLayer := incomingPacket.TransportLayer(); incomingTransportLayer != nil {
			incomingTransportLayerContents = incomingTransportLayer.LayerContents()
		}

		if iapp := incomingPacket.ApplicationLayer(); iapp != nil {
			incomingPayload = iapp.Payload()
		}

		connValue := RawPacket{
			Connection: connectionIDString,
			IPPacket:   incomingNetworkLayerContents,
			TCPPacket:  incomingTransportLayerContents,
			Payload: incomingPayload,
			Timestamp:  incomingTime,
			AllowBlock: allowBlock,
			InOut: true,
			Handshake: false,
		}

		rethinkdb.DB(transport).Table(packetsKey).Insert(connValue).RunWrite(client.session)
	}

	// If  there is an outgoing packet, be sure to save that packet and its timestamp too.
	for  _, outgoingPacket := range conn.Outgoing {
		var outgoingNetworkLayerContents []byte
		var outgoingTransportLayerContents []byte
		var outgoingPayload []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		//fmt.Println(outgoingNetworkLayerContents)
		//fmt.Println(outgoingTransportLayerContents)
		//fmt.Println(outgoingTime)

		if outgoingNetworkLayer := outgoingPacket.NetworkLayer(); outgoingNetworkLayer != nil {
			outgoingNetworkLayerContents = outgoingNetworkLayer.LayerContents()
		}

		if outgoingTransportLayer := outgoingPacket.TransportLayer(); outgoingTransportLayer != nil {
			outgoingTransportLayerContents = outgoingTransportLayer.LayerContents()
		}

		if oapp := outgoingPacket.ApplicationLayer(); oapp != nil {
			outgoingPayload = oapp.Payload()
		}

		connValue := RawPacket{
			Connection: connectionIDString,
			IPPacket:   outgoingNetworkLayerContents,
			TCPPacket:  outgoingTransportLayerContents,
			Payload: outgoingPayload,
			Timestamp:  outgoingTime,
			AllowBlock: allowBlock,
			InOut: false,
			Handshake: false,
		}

		rethinkdb.DB(transport).Table(packetsKey).Insert(connValue).RunWrite(client.session)
	}
}

// AddTrainPacket adds a packet to the training data set
func (client Client) AddTrainPacket(transport string, allowBlock bool, conn ConnectionPackets) {
	var connValue RawPacket

	connectionIDString := makeConnectionID()
	incomingPacket := conn.Incoming

	var incomingNetworkLayerContents []byte
	var incomingTransportLayerContents []byte
	var incomingPayload []byte
	var incomingTime = incomingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

	//fmt.Println(incomingPayload)
	//fmt.Println(incomingTime)

	if incomingNetworkLayer := incomingPacket.NetworkLayer(); incomingNetworkLayer != nil {
		incomingNetworkLayerContents = incomingNetworkLayer.LayerContents()
	}

	if incomingTransportLayer := incomingPacket.TransportLayer(); incomingTransportLayer != nil {
		incomingTransportLayerContents = incomingTransportLayer.LayerContents()
	}

	if iapp := incomingPacket.ApplicationLayer(); iapp != nil {
		incomingPayload = iapp.Payload()
	}

	_, createErr := rethinkdb.DBCreate(transport).RunWrite(client.session)
	if createErr == nil {
		fmt.Println("-> Created database", transport)
	}

	_, tableCreateErr := rethinkdb.DB(transport).TableCreate(packetsKey).RunWrite(client.session)
	if tableCreateErr == nil {
		fmt.Println("-> Created table", transport, packetsKey)
	}

	connValue = RawPacket{
		Connection: connectionIDString,
		IPPacket:   incomingNetworkLayerContents,
		TCPPacket:  incomingTransportLayerContents,
		Payload:    incomingPayload,
		Timestamp:  incomingTime,
		AllowBlock: allowBlock,
		InOut:      true,
		Handshake: true,
	}

	rethinkdb.DB(transport).Table(packetsKey).Insert(connValue).RunWrite(client.session)

	// In some cases we will get a conn that only has an incoming packet
	// This should only ever happen if the conn is blocked
	// Here we save our incoming packet and connection ID
	if  conn.Outgoing != nil {
		outgoingPacket := conn.Outgoing

		var outgoingNetworkLayerContents []byte
		var outgoingTransportLayerContents []byte
		var outgoingPayload []byte
		var outgoingTime = outgoingPacket.Metadata().CaptureInfo.Timestamp.UnixNano() / 1000000

		//fmt.Println(outgoingPayload)
		//fmt.Println(outgoingTime)

		if outgoingNetworkLayer := outgoingPacket.NetworkLayer(); outgoingNetworkLayer != nil {
			outgoingNetworkLayerContents = outgoingNetworkLayer.LayerContents()
		}

		if outgoingTransportLayer := outgoingPacket.TransportLayer(); outgoingTransportLayer != nil {
			outgoingTransportLayerContents = outgoingTransportLayer.LayerContents()
		}

		if oapp := outgoingPacket.ApplicationLayer(); oapp != nil {
			outgoingPayload = oapp.Payload()
		}

		connValue = RawPacket{
			Connection: connectionIDString,
			IPPacket:   outgoingNetworkLayerContents,
			TCPPacket:  outgoingTransportLayerContents,
			Payload:    outgoingPayload,
			Timestamp:  outgoingTime,
			AllowBlock: allowBlock,
			InOut:      false,
			Handshake: true,
		}

		rethinkdb.DB(transport).Table(packetsKey).Insert(connValue).RunWrite(client.session)
	}
}
