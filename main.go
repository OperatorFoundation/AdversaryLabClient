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
	fmt.Println("Adversary Lab Client is running...")
	var allowBlock bool
	var allowBlockChannel = make(chan bool)

	if len(os.Args) < 2 {
		usage()
		return
	}

	port := os.Args[1]

	if len(os.Args) == 2 {
		//streamMode = false
		var allowBlockWasSet = false
		go capture(port, allowBlockChannel, nil)

		for allowBlockWasSet == false {
			fmt.Print("Type allow or block when you are done recording: ")
			reader := bufio.NewReader(os.Stdin)
			text, _ := reader.ReadString('\n')
			text = strings.Replace(text, "\n", "", -1)
			if text == "allow" {
				allowBlock = true
				allowBlockWasSet = true
			} else if text == "block" {
				allowBlock = false
				allowBlockWasSet = true
			}
		}

		// This tells us that we are done recording and the buffered packets
		// are either allowed or blocked based on user input.
		allowBlockChannel<-allowBlock

	} else if len(os.Args) == 3 {
		//streamMode = true

		if os.Args[2] == "allow" {
			allowBlock = true
		}

		capture(port, allowBlockChannel, &allowBlock)
	} else {
		usage()
		return
	}
}

func capture(port string, allowBlockChannel chan bool, allowBlock *bool) {
	var lab protocol.Client
	var err error
	var input string

	fmt.Println("Launching server...")

	lab = protocol.Connect()

	captured := map[Connection]protocol.ConnectionPackets{}

	var handle *pcap.Handle
	var pcapErr error

	switch runtime.GOOS {
	case "darwin":
		handle, pcapErr = pcap.OpenLive("en0", 1024, false, 30*time.Second)
		if pcapErr != nil {
			fmt.Println("Error opening network device:")
			fmt.Println(pcapErr)
			if handle != nil {
				handle.Close()
			}
			os.Exit(1)
		}
	default:
		handle, pcapErr = pcap.OpenLive("eth0", 1024, false, 30*time.Second)
		if pcapErr != nil {
			fmt.Println("Error opening network device:")
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
	saveCaptured(lab, allowBlock, allowBlockChannel, recordable)
}

func usage() {
	fmt.Println("AdversaryLabClient <port> [protocol]")
	fmt.Println("Example: AdversaryLabClient 80 allow")
	fmt.Println("Example: AdversaryLabClient 443 block")
	fmt.Println("Example: AdversaryLabClient 80")
	fmt.Println("Example: AdversaryLabClient 443")
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
	fmt.Println("Capturing port", port)

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
			//			fmt.Print(".")
			//fmt.Println(packet)

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
	fmt.Println("reading packets")
	for packet := range packetSource.Packets() {
		//fmt.Println("readPacket")
		packetChannel <- packet
	}
	fmt.Println("done reading packets")
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
			fmt.Println("First packet recorded.")
			if incoming {
				connPackets = protocol.ConnectionPackets{Incoming: packet, Outgoing: nil}
				captured[conn] = connPackets
			}
		} else { // This is the second packet of the connection
			if !incoming && connPackets.Outgoing == nil {
				fmt.Println("Second packet seen.")
				connPackets.Outgoing = packet
				captured[conn] = connPackets

				if recordable != nil {
					fmt.Print(".")
					recordable <- connPackets
				} else {
					fmt.Println("Second packet seen channel is closed.")
				}

			}
		}
	}
}

func saveCaptured(lab protocol.Client, allowBlock *bool, stopCapturing chan bool, recordable chan protocol.ConnectionPackets) {
	fmt.Println("Saving captured byte sequences... ")

	// Use the buffer if we are not in streaming mode
	buffer := make([]protocol.ConnectionPackets, 0)

	for {
		select {
		case newAllowBlock := <-stopCapturing:
			// Save buffered packets and quit
			for _, packet := range buffer{
				fmt.Print("*")
				lab.AddTrainPacket(newAllowBlock, packet)
			}
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
