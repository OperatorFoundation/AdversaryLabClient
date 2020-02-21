package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"

	"github.com/OperatorFoundation/AdversaryLabClient/protocol"
)

type Connection struct {
	small layers.TCPPort
	big   layers.TCPPort
}

func NewConnection(packet *layers.TCP) Connection {
	if packet.SrcPort < packet.DstPort {
		return Connection{small: packet.SrcPort, big: packet.DstPort}
	} else {
		return Connection{small: packet.DstPort, big: packet.SrcPort}
	}
}

func (conn Connection) CheckPort(port layers.TCPPort) bool {
	return conn.small == port || conn.big == port
}

func main() {
	fmt.Println("-> Adversary Lab Client is running...Now with RethinkDB support!")
	var allowBlock bool
	var allowBlockChannel = make(chan bool)
	var signalChannel = make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	go handleSignals(allowBlockChannel, signalChannel)

	if len(os.Args) < 3 {
		usage()
		return
	}

	transport := os.Args[1]
	port := os.Args[2]

	if len(os.Args) == 3 {
		// Buffering Mode
		// The user has not yet indicated which category this data belongs to.
		// Buffer the data until the user enters 'allowed' or 'blocked'.
		go listenForDataCategory(allowBlockChannel)
		capture(transport, port, allowBlockChannel, nil)
	} else if len(os.Args) == 4 {
		// Streaming Mode
		// The user has indicated how this data should be categorized.
		// Save the data as we go using the indicated category.
		if os.Args[3] == "allow" {
			allowBlock = true
		} else if os.Args[3] == "block" {
			allowBlock = false
		} else {
			usage()
			return
		}

		capture(transport, port, allowBlockChannel, &allowBlock)
	} else {
		usage()
		return
	}
}

func handleSignals (allowBlockChannel chan bool, signalChannel chan os.Signal) {
	// Wait until we get the interrupt signal(ctrl + c) from the user
	_ = <-signalChannel
	// Set allow block to false so that the program exits gracefully
	allowBlockChannel <- false
}

func listenForDataCategory(allowBlockChannel chan bool) {
	var allowBlockWasSet = false
	allowBlock := false
	reader := bufio.NewReader(os.Stdin)

	for allowBlockWasSet == false {
		fmt.Print("-> Type 'allow' or 'block' when you are done recording <-\n")
		text, readErr := reader.ReadString('\n')
		if readErr != nil {
			allowBlock = false
			allowBlockWasSet = true
			break
		}

		text = strings.Replace(text, "\n", "", -1)
		if text == "allow" {
			fmt.Println("-> This packet data will be saved as allowed.")
			allowBlock = true
			allowBlockWasSet = true
		} else if text == "block" {
			fmt.Println("-> This packet data will be saved as blocked.")
			allowBlock = false
			allowBlockWasSet = true
		} else {
			fmt.Printf("-> Received unexpected input for the connection data category please enter 'allowed' or 'blocked':\n %s", text)
		}
	}

	// This tells us that we are done recording and the buffered packets
	// are either allowed or blocked based on user input.
	allowBlockChannel<-allowBlock
}

func listenForEnter(allowBlockChannel chan bool) {
	var allowBlockWasSet = false
	allowBlock := false

	for allowBlockWasSet == false {
		fmt.Print("-> Type 'allow' or 'block' when you are done recording <-\n")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		if text == "allow" {
			fmt.Println("-> This packet data will be saved as allowed.")
			allowBlock = true
			allowBlockWasSet = true
		} else if text == "block" {
			fmt.Println("-> This packet data will be saved as blocked.")
			allowBlock = false
			allowBlockWasSet = true
		} else {
			fmt.Printf("-> Received unexpected input for the connection data category please enter 'allowed' or 'blocked':\n %s", text)
		}
	}

	// This tells us that we are done recording and the buffered packets
	// are either allowed or blocked based on user input.
	allowBlockChannel<-allowBlock
}

func capture(transport string, port string, allowBlockChannel chan bool, allowBlock *bool) {
	var err error
	var input string

	fmt.Println("-> Launching server...")

	lab, connectErr := protocol.Connect()
	if connectErr != nil {
		fmt.Println("-> Connect error!", connectErr.Error())
		return
	}

	fmt.Println("-> Connected.")

	captured := map[Connection]protocol.ConnectionPackets{}
	rawCaptured := map[Connection]protocol.RawConnectionPackets{}

	var handle *pcap.Handle
	var pcapErr error

	switch runtime.GOOS {
	case "darwin":
		handle, pcapErr = pcap.OpenLive("en0", 1024, false, 30*time.Second)
		if pcapErr != nil {
			fmt.Println("-> Error opening network device:")
			fmt.Println(pcapErr)
			if handle != nil {
				handle.Close()
			}
			os.Exit(1)
		}
	default:
		handle, pcapErr = pcap.OpenLive("ens18", 1024, false, 30*time.Second)
		if pcapErr != nil {
			fmt.Println("-> Error opening network device:")
			fmt.Println(pcapErr)
			if handle != nil {
				handle.Close()
			}
			os.Exit(1)
		}
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packetChannel := make(chan gopacket.Packet)
	go readPackets(packetSource, packetChannel)

	// stopDetecting := make(chan bool)
	// ports := mapset.NewSet()
	// go detectPorts(ports, packetChannel, captured, stopDetecting)

	var selectedPort layers.TCPPort
	var temp uint64

	input = port

	temp, err = strconv.ParseUint(strings.TrimSpace(input), 10, 16)
	CheckError(err)
	selectedPort = layers.TCPPort(temp)

	recordable := make(chan protocol.ConnectionPackets)

	go capturePort(selectedPort, packetChannel, captured, rawCaptured, allowBlockChannel, recordable)
	saveCaptured(*lab, transport, allowBlock, allowBlockChannel, recordable, captured, rawCaptured)
}

func usage() {
	fmt.Println("-> AdversaryLabClient <transport> <port> [protocol]")
	fmt.Println("-> Example: AdversaryLabClient HTTP 80 allow")
	fmt.Println("-> Example: AdversaryLabClient HTTPS 443 block")
	fmt.Println("-> Example: AdversaryLabClient HTTP 80")
	fmt.Println("-> Example: AdversaryLabClient HTTPS 443")
	fmt.Println()
	os.Exit(1)
}

/* A Simple function to verify error */
func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

func capturePort(port layers.TCPPort, packetChannel chan gopacket.Packet, captured map[Connection]protocol.ConnectionPackets, rawCaptured map[Connection]protocol.RawConnectionPackets, stopCapturing chan bool, recordable chan protocol.ConnectionPackets) {
	fmt.Println("-> Capturing port", port)

	var count = uint16(len(captured))

	for {
		select {
		case <-stopCapturing:
			return
		case packet := <-packetChannel:
			// Let's see if the packet is TCP
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			var app = packet.ApplicationLayer()
			if tcpLayer != nil {
				tcp, _ := tcpLayer.(*layers.TCP)

				conn := NewConnection(tcp)
				if !conn.CheckPort(port) {
					continue
				}

				recordRawPacket(packet, rawCaptured, port)

				if app != nil {
					recordPacket(packet, captured, recordable, port)

					newCount := uint16(len(captured))
					if newCount > count {
						count = newCount
					}
				}

			} else {
				// fmt.Println("No TCP")
				// fmt.Println(packet)
			}
		}
	}
}

func readPackets(packetSource *gopacket.PacketSource, packetChannel chan gopacket.Packet) {
	for packet := range packetSource.Packets() {
		packetChannel <- packet
	}
}

// func discardUnusedPorts(port layers.TCPPort, captured map[Connection]protocol.ConnectionPackets) {
// 	for conn := range captured {
// 		if !conn.CheckPort(port) {
// 			delete(captured, conn)
// 		}
// 	}
// }


func recordRawPacket(packet gopacket.Packet, captured map[Connection]protocol.RawConnectionPackets, port layers.TCPPort) {

	fmt.Println("Entered recordRawPacket")
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		conn := NewConnection(tcp)
		incoming := packet.Layer(layers.LayerTypeTCP).(*layers.TCP).DstPort == port
		connPackets, ok := captured[conn]

		if !ok {
			connPackets = protocol.RawConnectionPackets{
				Incoming: make([]gopacket.Packet, 0),
				Outgoing: make([]gopacket.Packet, 0),
			}
		}

		if incoming {
			connPackets.Incoming = append(connPackets.Incoming, packet)
			captured[conn] = connPackets
		} else {
			connPackets.Outgoing = append(connPackets.Outgoing, packet)
			captured[conn] = connPackets
		}
	}
}

func recordPacket(packet gopacket.Packet, captured map[Connection]protocol.ConnectionPackets, recordable chan protocol.ConnectionPackets, port layers.TCPPort) {
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer != nil {
		//fmt.Println("TCP layer recorded.")
		tcp, _ := tcpLayer.(*layers.TCP)
		conn := NewConnection(tcp)
		incoming := packet.Layer(layers.LayerTypeTCP).(*layers.TCP).DstPort == port
		connPackets, ok := captured[conn]

		// This is the first packet of the connection
		if !ok {
			if incoming {
				connPackets = protocol.ConnectionPackets{Incoming: packet, Outgoing: nil}
				captured[conn] = connPackets
			}
		} else {
			// This is the second packet of the connection
			if !incoming && connPackets.Outgoing == nil {
				connPackets.Outgoing = packet
				captured[conn] = connPackets

				if recordable != nil {
					fmt.Println("-> .")
					recordable <- connPackets
				} else {
					fmt.Println("-> Second packet seen channel is closed.")
				}
			}
		}
	}
}

// If partial allowed throw it out
// If partial blocked save it
// Add
func saveCaptured(lab protocol.Client, transport string, allowBlock *bool, stopCapturing chan bool, recordable chan protocol.ConnectionPackets, captured map[Connection]protocol.ConnectionPackets, rawCaptured map[Connection]protocol.RawConnectionPackets) {
	fmt.Println("-> Saving captured raw connection packets... ")

	// Use the buffer if we are not in streaming mode
	buffer := make([]protocol.ConnectionPackets, 0)

	for {
		select {
		case newAllowBlock := <-stopCapturing:
			// Save buffered connections that are complete (have both incoming and outgoing packets) and quit
			for _, packet := range buffer {
				println("-> Saving complete connections. --<-@")
				lab.AddTrainPacket(transport, newAllowBlock, packet)
				time.Sleep(8)
			}

			for _, rawConnection := range rawCaptured {
				println("-> Saving complete raw connections. --<-@")
				lab.AddRawTrainPacket(transport, newAllowBlock, rawConnection)
				time.Sleep(8)
			}

			// Usually we want both incoming and outgoing packets
			// In the case where we know these are blocked connections
			// We want to record the data even when we have not received a response.
			// This is still a valid blocked case. We expect that some blocked connections will behave in this way.

			//If the connections in this map are labeled blocked by the user
			println("newAllowBlock is ", newAllowBlock)
			if newAllowBlock == false {
				println("-> Captured count is ", len(captured))
				for _, connection := range captured {
					println("Entering loop for saving incomplete connections.")
					// If this connection in the map is incomplete (only the incoming packet was captured) save it
					// Check this because a complete struct (both incoming and outgoing packets are populated)
					// will already be getting saved by the above for loop
					if connection.Outgoing == nil {
						fmt.Println("-> Saving incomplete connection.  --<-@")
						lab.AddTrainPacket(transport, newAllowBlock, connection)
					}
				}
			}

			fmt.Print("--> We are done saving things to the database. Bye now!\n")
			os.Exit(1)
		case connPackets := <-recordable:
			if allowBlock == nil{
				buffer = append(buffer, connPackets)
			} else {
				fmt.Print("*")
				lab.AddTrainPacket(transport, *allowBlock, connPackets)
			}
		}
	}
}


