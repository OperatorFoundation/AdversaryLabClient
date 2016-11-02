package main

import (
	//	"fmt"
	"fmt"
	"runtime"

	"github.com/blanu/AdversaryLab-nanomsg/services"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	updates := make(chan services.Update, 100)

	fmt.Println("*** INIT")

	packet := services.NewPacketService("tcp://localhost:4567", updates)
	fmt.Println("2")
	rule := services.NewRuleService("tcp://localhost:4568", updates)

	fmt.Println("*** RUN")

	go packet.Run()
	rule.Run()

	fmt.Println("*** FINISHED")
}
