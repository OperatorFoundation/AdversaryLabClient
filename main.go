package main

import (
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
	var mode string
	//	var captureName string
	var dataset string

	if len(os.Args) < 3 {
		usage()
	}

	mode = os.Args[1]

	if mode == "capture" {
		dataset = os.Args[2]

		var allowBlock = false
		if os.Args[3] == "allow" {
			allowBlock = true
		}

		if len(os.Args) > 4 {
			capture(dataset, allowBlock, &os.Args[4])
		} else {
			capture(dataset, allowBlock, nil)
		}
		// } else if mode == "rules" {
		// 	rules(captureName)
	} else {
		usage()
	}
}

func capture(dataset string, allowBlock bool, port *string) {
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

	input = *port

	temp, err = strconv.ParseUint(strings.TrimSpace(input), 10, 16)
	CheckError(err)
	selectedPort = layers.TCPPort(temp)

	stopCapturing := make(chan bool)
	recordable := make(chan protocol.ConnectionPackets)
	go capturePort(selectedPort, packetChannel, captured, stopCapturing, recordable)
	saveCaptured(lab, dataset, allowBlock, stopCapturing, recordable)
}

func usage() {
	fmt.Println("AdversaryLabClient capture [protocol] [dataset] <port>")
	fmt.Println("Example: AdversaryLabClient capture testing allow")
	fmt.Println("Example: AdversaryLabClient capture testing allow 80")
	fmt.Println("Example: AdversaryLabClient capture testing block")
	fmt.Println("Example: AdversaryLabClient capture testing block 443")
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
			app := packet.ApplicationLayer()
			if tcpLayer != nil && app != nil {
				//		        fmt.Println("TCP layer captured.")
				tcp, _ := tcpLayer.(*layers.TCP)

				conn := NewConnection(tcp)
				if !conn.CheckPort(layers.TCPPort(port)) {
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
		fmt.Println("readPacket")
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
		fmt.Println("TCP layer recorded.")
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
		} else { // This is the second packet of the connection
			if !incoming && connPackets.Outgoing == nil {
				connPackets.Outgoing = packet
				captured[conn] = connPackets

				if recordable != nil {
					fmt.Print(".")
					recordable <- connPackets
				}
			}
		}
	}
}

func saveCaptured(lab protocol.Client, dataset string, allowBlock bool, stopCapturing chan bool, recordable chan protocol.ConnectionPackets) {
	fmt.Println("Saving captured byte sequences... ")

	for {
		select {
		case <-stopCapturing:
			return // FIXME - empty channel of pending packets, but don't block
		case connPackets := <-recordable:
			fmt.Print("*")
			lab.AddTrainPacket(dataset, allowBlock, connPackets)
		}
	}
}
