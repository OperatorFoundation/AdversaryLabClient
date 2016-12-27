package services

import (
	"fmt"

	"github.com/ugorji/go/codec"

	"github.com/blanu/AdversaryLab-nanomsg/freefall"
	"github.com/blanu/AdversaryLab-protocol/adversarylab"
)

type Handlers struct {
	handlers map[string]*StoreHandler
	updates  chan Update
}

// StoreHandler is a request handler that knows about storage
type StoreHandler struct {
	path  string
	store *freefall.Store
	//	seqs          *freefall.SequenceMap
	offseqs       *freefall.OffsetSequenceMap
	updates       chan Update
	ruleUpdates   chan *freefall.RuleCandidate
	handleChannel chan *adversarylab.TrainPacket
}

type TrainService struct {
	handlers Handlers
	serve    adversarylab.Server
}

type Update struct {
	Path string
	Rule *freefall.RuleCandidate
}

func NewTrainPacketService(listenAddress string, updates chan Update) *TrainService {
	handlers := Handlers{handlers: make(map[string]*StoreHandler), updates: updates}
	// files, err := ioutil.ReadDir("store")
	// if err != nil {
	// 	fmt.Println("Failed to read store directory", err)
	// } else {
	// 	for _, file := range files {
	// 		handlers.Load(file.Name())
	// 	}
	// }

	serve := adversarylab.Listen(listenAddress)

	return &TrainService{handlers: handlers, serve: serve}
}

func (self *TrainService) Run() {
	for {
		fmt.Println("accepting reqresp")
		self.serve.Accept(self.handlers.Handle)
		fmt.Println("accepted reqresp")
	}
}

func (self Handlers) Load(name string) *StoreHandler {
	if handler, ok := self.handlers[name]; ok {
		return handler
	} else {
		store, err := freefall.OpenStore(name)
		if err != nil {
			fmt.Println("Error opening store")
			fmt.Println(err)
			return nil
		}

		// sm, err2 := freefall.NewSequenceMap(name)
		// if err2 != nil {
		// 	fmt.Println("Error opening bytemap")
		// 	fmt.Println(err2)
		// 	return nil
		// }

		ruleUpdates := make(chan *freefall.RuleCandidate, 10)

		osm, err2 := freefall.NewOffsetSequenceMap(name, ruleUpdates)
		if err2 != nil {
			fmt.Println("Error opening bytemap")
			fmt.Println(err2)
			return nil
		}

		handleChannel := make(chan *adversarylab.TrainPacket)

		handler := &StoreHandler{path: name, store: store, offseqs: osm, updates: self.updates, ruleUpdates: ruleUpdates, handleChannel: handleChannel}
		handler.Init()
		self.handlers[name] = handler
		return handler
	}
}

func (self Handlers) Handle(request []byte) []byte {
	fmt.Println("New packet")
	var name string

	var value = adversarylab.NamedType{}
	var h = adversarylab.NamedTypeHandle()
	var dec = codec.NewDecoderBytes(request, h)
	var err = dec.Decode(&value)
	if err != nil {
		fmt.Println("Failed to decode")
		fmt.Println(err.Error())
		return []byte("success")
	}

	switch value.Name {
	case "adversarylab.TrainPacket":
		fmt.Println("Got packet")
		packet := adversarylab.TrainPacketFromMap(value.Value.(map[interface{}]interface{}))
		if packet.Incoming {
			name = packet.Dataset + "-incoming"
		} else {
			name = packet.Dataset + "-outgoing"
		}

		handler := self.Load(name)
		if handler != nil {
			handler.handleChannel <- &packet
			return []byte("success")
		} else {
			fmt.Println("Could not load handler for", name)
			return []byte("success")
		}
	default:
		fmt.Println("Unknown request type")
		fmt.Println(value)
		fmt.Println("<.>")
		return []byte("success")
	}
}

// Init process all items that are already in storage
func (self *StoreHandler) Init() {
	fmt.Println("Loading")
	// FIXME - loading of Last value
	//	self.Load()
	fmt.Println("Processing")
	go self.HandleChannel(self.handleChannel)
	go self.HandleRuleUpdatesChannel(self.ruleUpdates)
	//	self.store.FromIndexDo(self.store.LastIndex(), self.processChannel)
}

func (self *StoreHandler) HandleChannel(ch chan *adversarylab.TrainPacket) {
	for request := range ch {
		fmt.Print(".")
		self.Handle(request)
	}
}

func (self *StoreHandler) HandleRuleUpdatesChannel(ch chan *freefall.RuleCandidate) {
	for rule := range ch {
		fmt.Print(":")
		update := Update{Path: self.path, Rule: rule}
		self.updates <- update
	}
}

// Handle handles requests
func (self *StoreHandler) Handle(request *adversarylab.TrainPacket) []byte {
	index := self.store.Add(request.Payload)
	record, err := self.store.GetRecord(index)
	if err != nil {
		fmt.Println("Error getting new record", err)
	} else {
		self.Process(request.AllowBlock, record)
	}

	return []byte("success")
}

// Process processes records
func (self *StoreHandler) Process(allowBlock bool, record *freefall.Record) {
	fmt.Println("Processing", record.Index)

	if record.Index < self.store.LastIndex() {
		fmt.Println("Rejecting duplicate", record.Index, "<", self.store.LastIndex())
		return
	}

	// FIXME - process bytes into bytemaps

	fmt.Println("Sending update")
	go self.processBytes(allowBlock, record.Data)
	self.updates <- Update{Path: self.path}
}

func (self *StoreHandler) processBytes(allowBlock bool, bytes []byte) {
	//	self.seqs.ProcessBytes(allowBlock, bytes)
	self.offseqs.ProcessBytes(allowBlock, bytes)
}
