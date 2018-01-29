package services

import (
	"fmt"

	"github.com/ugorji/go/codec"

	"github.com/OperatorFoundation/AdversaryLab/protocol"
	"github.com/OperatorFoundation/AdversaryLab/storage"
)

type Handlers struct {
	handlers   map[string]*StoreHandler
	updates    chan Update
	storeCache *storage.StoreCache
}

// StoreHandler is a request handler that knows about storage
type StoreHandler struct {
	path  string
	store *storage.Store
	//	seqs          *storage.SequenceMap
	offseqs       *storage.OffsetSequenceMap
	lengths       *storage.LengthCounter
	updates       chan Update
	ruleUpdates   chan *storage.RuleCandidate
	handleChannel chan *protocol.TrainPacket
}

type TrainService struct {
	handlers Handlers
	serve    protocol.Server
}

type Update struct {
	Path string
	Rule *storage.RuleCandidate
}

func NewTrainPacketService(listenAddress string, updates chan Update, storeCache *storage.StoreCache) *TrainService {
	handlers := Handlers{handlers: make(map[string]*StoreHandler), updates: updates, storeCache: storeCache}
	// files, err := ioutil.ReadDir("store")
	// if err != nil {
	// 	fmt.Println("Failed to read store directory", err)
	// } else {
	// 	for _, file := range files {
	// 		handlers.Load(file.Name())
	// 	}
	// }

	serve := protocol.Listen(listenAddress)

	return &TrainService{handlers: handlers, serve: serve}
}

func (self *TrainService) Run() {
	for {
		//		fmt.Println("accepting reqresp")
		self.serve.Accept(self.handlers.Handle)
		//		fmt.Println("accepted reqresp")
	}
}

func (self Handlers) Load(name string) *StoreHandler {
	var err error

	if handler, ok := self.handlers[name]; ok {
		return handler
	} else {
		store := self.storeCache.Get(name)
		if store == nil {
			store, err = storage.OpenStore(name)
			if err != nil {
				return nil
			}

			self.storeCache.Put(name, store)
		}

		// sm, err2 := storage.NewSequenceMap(name)
		// if err2 != nil {
		// 	fmt.Println("Error opening bytemap")
		// 	fmt.Println(err2)
		// 	return nil
		// }

		ruleUpdates := make(chan *storage.RuleCandidate, 10)

		osm, err2 := storage.NewOffsetSequenceMap(name, ruleUpdates)
		if err2 != nil {
			return nil
		}

		lengthCounter, err := storage.NewLengthCounter(name)
		if err != nil {
			fmt.Println("Could not initialize length counter")
		}

		handleChannel := make(chan *protocol.TrainPacket)

		handler := &StoreHandler{path: name, store: store, offseqs: osm, lengths: lengthCounter, updates: self.updates, ruleUpdates: ruleUpdates, handleChannel: handleChannel}
		handler.Init()
		self.handlers[name] = handler
		return handler
	}
}

func (self Handlers) Handle(request []byte) []byte {
	//	fmt.Println("New packet")
	var name string

	var value = protocol.NamedType{}
	var h = protocol.NamedTypeHandle()
	var dec = codec.NewDecoderBytes(request, h)
	var err = dec.Decode(&value)
	if err != nil {
		fmt.Println("Failed to decode")
		fmt.Println(err.Error())
		return []byte("success")
	}

	switch value.Name {
	case "protocol.TrainPacket":
		//		fmt.Println("Got packet")
		packet := protocol.TrainPacketFromMap(value.Value.(map[interface{}]interface{}))
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
	//	fmt.Println("Loading")
	// FIXME - loading of Last value
	//	self.Load()
	//	fmt.Println("Processing")
	go self.HandleChannel(self.handleChannel)
	go self.HandleRuleUpdatesChannel(self.ruleUpdates)
	//	self.store.FromIndexDo(self.store.LastIndex(), self.processChannel)
}

func (self *StoreHandler) HandleChannel(ch chan *protocol.TrainPacket) {
	for request := range ch {
		if !storage.Debug {
			fmt.Print(".")
		}
		self.Handle(request)
	}
}

func (self *StoreHandler) HandleRuleUpdatesChannel(ch chan *storage.RuleCandidate) {
	for rule := range ch {
		update := Update{Path: self.path, Rule: rule}
		//		fmt.Println("training sending update", update)
		self.updates <- update
	}
}

// Handle handles requests
func (self *StoreHandler) Handle(request *protocol.TrainPacket) []byte {

	self.store.Add(request.Payload)
	self.Process(request.AllowBlock, request.Payload)

	return []byte("success")
}

// Process processes records
func (self *StoreHandler) Process(allowBlock bool, data []byte) {
	self.processBytes(allowBlock, data)
}

func (self *StoreHandler) processBytes(allowBlock bool, bytes []byte) {
	//	self.seqs.ProcessBytes(allowBlock, bytes)
	self.offseqs.ProcessBytes(allowBlock, bytes)
	self.lengths.ProcessBytes(allowBlock, bytes)
}
