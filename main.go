package main

import (
	"bufio"
	"fmt"
	"os"
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
	fmt.Println("-> Adversary Lab Client is running...")
	var allowBlock bool
	var allowBlockChannel = make(chan bool)

	if len(os.Args) < 2 {
		usage()
		return
	}

	port := os.Args[1]

	if len(os.Args) == 2 {
		// Buffering Mode
		// The user has not yet indicated which category this data belongs to.
		// Buffer the data until the user enters 'allowed' or 'blocked'.
		go listenForDataCategory(allowBlock, allowBlockChannel)
		capture(port, allowBlockChannel, nil)
	} else if len(os.Args) == 3 {
		// Streaming Mode
		// The user has indicated how this data should be categorized.
		// Save the data as we go using the indicated category.
		if os.Args[2] == "allow" {
			allowBlock = true
		} else if os.Args[2] == "block" {
			allowBlock = false
		} else {
			usage()
			return
		}

		capture(port, allowBlockChannel, &allowBlock)
	} else {
		usage()
		return
	}
}

func listenForDataCategory(allowBlock bool, allowBlockChannel chan bool) {
	var allowBlockWasSet = false

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

func capture(port string, allowBlockChannel chan bool, allowBlock *bool) {
	var lab protocol.Client
	var err error
	var input string

	fmt.Println("-> Launching server...")

	lab = protocol.Connect()

	captured := map[Connection]protocol.ConnectionPackets{}

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

	go capturePort(selectedPort, packetChannel, captured, allowBlockChannel, recordable)
	saveCaptured(lab, allowBlock, allowBlockChannel, recordable, captured)
}

func usage() {
	fmt.Println("-> AdversaryLabClient <port> [protocol]")
	fmt.Println("-> Example: AdversaryLabClient 80 allow")
	fmt.Println("-> Example: AdversaryLabClient 443 block")
	fmt.Println("-> Example: AdversaryLabClient 80")
	fmt.Println("-> Example: AdversaryLabClient 443")
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

func capturePort(port layers.TCPPort, packetChannel chan gopacket.Packet, captured map[Connection]protocol.ConnectionPackets, stopCapturing chan bool, recordable chan protocol.ConnectionPackets) {
	fmt.Println("-> Capturing port", port)

	var count = uint16(len(captured))

	// for _, packet := range captured {
	// 	recordable <- packet
	// }

	for {
		//		fmt.Println("capturing...", port, count)
		select {
		case <-stopCapturing:
			return
		case packet := <-packetChannel:
			// Let's see if the packet is TCP
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			var app = packet.ApplicationLayer()
			if tcpLayer != nil && app != nil {
				//		        fmt.Println("TCP layer captured.")
				tcp, _ := tcpLayer.(*layers.TCP)

				conn := NewConnection(tcp)
				if !conn.CheckPort(port) {
					continue
				}

				recordPacket(packet, captured, recordable, port)

				newCount := uint16(len(captured))
				if newCount > count {
					count = newCount
					//					fmt.Print(count)
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
func saveCaptured(lab protocol.Client, allowBlock *bool, stopCapturing chan bool, recordable chan protocol.ConnectionPackets, captured map[Connection]protocol.ConnectionPackets) {
	fmt.Println("-> Saving captured byte sequences... ")

	// Use the buffer if we are not in streaming mode
	buffer := make([]protocol.ConnectionPackets, 0)

	for {
		select {
		case newAllowBlock := <-stopCapturing:
			// Save buffered connections that are complete (have both incoming and outgoing packets) and quit
			for _, packet := range buffer {
				fmt.Println("-> Saving complete connections.")
				lab.AddTrainPacket(newAllowBlock, packet)
				time.Sleep(10)
			}

			// Usually we want both incoming and outgoing packets
			// In the case where we know these are blocked connections
			// We want to record the data even when we have not received a response.
			// This is still a valid blocked case. We expect that some blocked connections will behave in this way.

			//If the connections in this map are labeled blocked by the user
			println("newAllowBlock is %t", newAllowBlock)
			if newAllowBlock == false {
				println("-> Captured count is %d", len(captured))
				for _, connection := range captured {
					println("Entering loop for saving incomplete connections.")
					// If this connection in the map is incomplete (only the incoming packet was captured) save it
					// Check this because a complete struct (both incoming and outgoing packets are populated)
					// will already be getting saved by the above for loop
					if connection.Outgoing == nil {
						fmt.Println("-> Saving incomplete connection.")
						lab.AddTrainPacket(newAllowBlock, connection)
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
				lab.AddTrainPacket(*allowBlock, connPackets)
			}
		}
	}

}
