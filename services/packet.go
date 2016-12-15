package services

import (
	"fmt"
	"io/ioutil"

	"github.com/streamrail/concurrent-map"
	"github.com/ugorji/go/codec"

	"github.com/blanu/AdversaryLab-nanomsg/freefall"
	"github.com/blanu/AdversaryLab-protocol/adversarylab"
)

type Handlers struct {
	handlers map[string]*StoreHandler
	mapper   func(*freefall.Record) *freefall.StoreData
	folder   func(*freefall.StoreData, *freefall.StoreData) *freefall.StoreData
	updates  chan Update
}

// StoreHandler is a request handler that knows about storage
type StoreHandler struct {
	path           string
	store          *freefall.Store
	value          *freefall.StoreData
	unmerged       cmap.ConcurrentMap
	mapper         func(*freefall.Record) *freefall.StoreData
	folder         func(*freefall.StoreData, *freefall.StoreData) *freefall.StoreData
	bytemap        *freefall.Bytemap
	updates        chan Update
	handleChannel  chan []byte
	processChannel chan *freefall.Record
}

type PacketService struct {
	handlers Handlers
	serve    adversarylab.Server
}

type Update struct {
	Path string
}

func NewPacketService(listenAddress string, updates chan Update) *PacketService {
	handlers := Handlers{handlers: make(map[string]*StoreHandler), mapper: mapLength, folder: foldLength, updates: updates}
	files, err := ioutil.ReadDir("store")
	if err != nil {
		fmt.Println("Failed to read store directory", err)
	} else {
		for _, file := range files {
			handlers.Load(file.Name())
		}
	}

	serve := adversarylab.Listen(listenAddress)

	return &PacketService{handlers: handlers, serve: serve}
}

func (self *PacketService) Run() {
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

		bytemap, err2 := freefall.NewBytemap(name)
		if err2 != nil {
			fmt.Println("Error opening bytemap")
			fmt.Println(err2)
			return nil
		}

		handleChannel := make(chan []byte)
		processChannel := make(chan *freefall.Record)

		handler := &StoreHandler{path: name, store: store, value: nil, unmerged: cmap.New(), mapper: self.mapper, folder: self.folder, bytemap: bytemap, updates: self.updates, handleChannel: handleChannel, processChannel: processChannel}
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
	case "adversarylab.Packet":
		fmt.Println("Got packet")
		packet := adversarylab.PacketFromMap(value.Value.(map[interface{}]interface{}))
		if packet.Incoming {
			name = packet.Protocol + "-" + packet.Dataset + "-incoming"
		} else {
			name = packet.Protocol + "-" + packet.Dataset + "-outgoing"
		}

		handler := self.Load(name)
		if handler != nil {
			handler.handleChannel <- packet.Payload
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
	self.Load()
	fmt.Println("Processing")
	go self.HandleChannel(self.handleChannel)
	go self.ProcessChannel(self.processChannel)
	self.store.FromIndexDo(self.value.Last, self.processChannel)
}

func (self *StoreHandler) HandleChannel(ch chan []byte) {
	for request := range ch {
		self.Handle(request)
	}
}

func (self *StoreHandler) ProcessChannel(ch chan *freefall.Record) {
	for request := range ch {
		self.Process(request)
	}
}

// Handle handles requests
func (self *StoreHandler) Handle(request []byte) []byte {
	index := self.store.Add(request)
	record, err := self.store.GetRecord(index)
	if err != nil {
		fmt.Println("Error getting new record", err)
	} else {
		self.processChannel <- record
	}

	return []byte("success")
}

// Process processes records
func (self *StoreHandler) Process(record *freefall.Record) {
	fmt.Println("Processing", record.Index)

	if record.Index < self.value.Last {
		fmt.Println("Rejecting duplicate", record.Index, "<", self.value.Last)
		return
	}

	if _, ok := self.unmerged.Get(string(record.Index)); ok {
		fmt.Println("Rejecting duplicate", record.Index, "in unmerged set")
		return
	}

	fmt.Println("Sending update")
	//go self.processBytes(record.Data)
	self.updates <- Update{Path: self.path}

	mapped := self.mapper(record)
	if record.Index == 0 {
		fmt.Println("Got index 0")
		self.value = mapped
	} else if record.Index-1 == self.value.Last {
		fmt.Println("Folding", self.value.Last, record.Index)
		self.value = self.folder(self.value, mapped)
		self.foldBackward()
	} else if _, ok := self.unmerged.Get(string(mapped.Last - 1)); ok {
		fmt.Println("Partial folding", mapped.First, mapped.Last)
		self.partialFoldBackward(mapped)
	} else {
		fmt.Println("Delay fold", record.Index, self.value.First, self.value.Last)
		self.unmerged.Set(string(record.Index), mapped)
		// It's too bad that ConcurrentMap only supports string keys
		// And that builtin Go maps are not concurrent
	}
}

func (self *StoreHandler) foldForward() {
	for {
		if next, ok := self.unmerged.Get(string(self.value.Last + 1)); ok {
			fmt.Println("Additional folding", self.value.Last, self.value.Last+1)
			self.value = self.folder(self.value, next.(*freefall.StoreData))
			self.unmerged.Remove(string(self.value.Last + 1))
			self.value.Last++
		} else {
			fmt.Println("No more folding", self.value.Last)
			self.Save()
			break
		}
	}
}

func (self *StoreHandler) foldBackward() {
	for {
		prevIndex := self.value.First - 1
		if prev, ok := self.unmerged.Get(string(prevIndex)); ok {
			self.unmerged.Remove(string(prevIndex))
			fmt.Println("Additional folding", prevIndex, self.value.First)
			self.value = self.folder(prev.(*freefall.StoreData), self.value)
			self.value.First = prevIndex
		} else {
			fmt.Println("No more folding", self.value.First, self.value.Last)
			self.Save()
			break
		}
	}
}

func (self *StoreHandler) partialFoldForward(mapped *freefall.StoreData) {
	for {
		if next, ok := self.unmerged.Get(string(mapped.Last + 1)); ok {
			nextMapped := next.(*freefall.StoreData)
			self.unmerged.Remove(string(nextMapped.First))
			fmt.Println("Partial folding", mapped.First, mapped.Last, nextMapped.First, nextMapped.Last)
			mapped = self.folder(mapped, nextMapped)
		} else {
			fmt.Println("No more partial folding", mapped.First, mapped.Last)
			self.unmerged.Set(string(mapped.First), mapped)
			break
		}
	}
}

func (self *StoreHandler) partialFoldBackward(mapped *freefall.StoreData) {
	for {
		prevIndex := self.value.First - 1
		if prev, ok := self.unmerged.Get(string(prevIndex)); ok {
			prevMapped := prev.(*freefall.StoreData)
			self.unmerged.Remove(string(prevMapped.Last))
			fmt.Println("Partial folding", mapped.First, mapped.Last, prevMapped.First, prevMapped.Last)
			mapped = self.folder(mapped, prevMapped)
		} else {
			fmt.Println("No more partial folding", mapped.First, mapped.Last)
			self.unmerged.Set(string(mapped.Last), mapped)
			break
		}
	}
}

func mapLength(record *freefall.Record) *freefall.StoreData {
	value := len(record.Data)
	result := [1500]int64{}
	result[value] = 1

	return &freefall.StoreData{First: record.Index, Last: record.Index, Data: result}
}

func foldLength(a *freefall.StoreData, b *freefall.StoreData) *freefall.StoreData {
	result := &freefall.StoreData{First: a.First, Last: b.Last, Data: [1500]int64{}}

	for i := 0; i < 1500; i++ {
		result.Data[i] = a.Data[i] + b.Data[i]
	}

	return result
}

// Load loads freefall.StoreData from storage
func (self *StoreHandler) Load() error {
	value, err := freefall.LoadStoreData(self.path)
	if err != nil {
		return err
	}

	self.value = value

	return nil
}

// Save saves StoreData to storage
func (self *StoreHandler) Save() error {
	return self.value.Save(self.path)
}
