package main

import (
	"fmt"
	"runtime"

	"github.com/blanu/AdversaryLab-nanomsg/freefall"
	"github.com/blanu/AdversaryLab-nanomsg/services"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	updates := make(chan services.Update, 100)

	fmt.Println("*** INIT")

	storeCache := freefall.NewStoreCache()

	train := services.NewTrainPacketService("tcp://localhost:4567", updates, storeCache)
	//	test := services.NewTestPacketService("tcp://localhost:4569", updates)
	fmt.Println("2")
	rule := services.NewRuleService("tcp://localhost:4568", updates, storeCache)

	fmt.Println("*** RUN")

	go train.Run()
	//	go test.Run()
	rule.Run()

	fmt.Println("*** FINISHED")
}
