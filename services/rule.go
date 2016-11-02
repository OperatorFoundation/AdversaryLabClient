package services

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/ugorji/go/codec"

	"github.com/blanu/AdversaryLab-nanomsg/freefall"
	"github.com/blanu/AdversaryLab-protocol/adversarylab"
)

type RuleHandlers struct {
	handlers map[string]*RuleHandler
	source   adversarylab.PubsubSource
}

// StoreHandler is a request handler that knows about storage
type RuleHandler struct {
	path           string
	store          *freefall.Store
	bytemap        *freefall.Bytemap
	cachedSequence []byte
}

type RuleService struct {
	handlers RuleHandlers
	serve    adversarylab.PubsubServer
	updates  chan Update
	source   adversarylab.PubsubSource
}

func NewRuleService(listenAddress string, updates chan Update) *RuleService {
	source := make(adversarylab.PubsubSource)

	fmt.Println("3")

	handlers := RuleHandlers{handlers: make(map[string]*RuleHandler), source: source}
	fmt.Println("3.5")
	files, err := ioutil.ReadDir("store")
	if err != nil {
		fmt.Println("Failed to read store directory", err)
	} else {
		for _, file := range files {
			fmt.Println("Loading", file.Name())
			handlers.Load(file.Name())
			fmt.Println("Loaded")
		}
	}

	fmt.Println("4")

	serve := adversarylab.PubsubListen(listenAddress, source)

	fmt.Println("5")

	return &RuleService{handlers: handlers, serve: serve, updates: updates, source: source}
}

func (self *RuleService) Run() {
	go self.handleUpdates()
	self.serve.Pump()
}

func (self RuleHandlers) Load(name string) *RuleHandler {
	if handler, ok := self.handlers[name]; ok {
		return handler
	} else {
		fmt.Println("new store")
		store, err := freefall.OpenReadonlyStore(name)
		if err != nil {
			fmt.Println("Error opening store")
			fmt.Println(err)
			return nil
		}

		fmt.Println("new bytemap")
		bytemap, err2 := freefall.NewBytemap(name)
		if err2 != nil {
			fmt.Println("Error opening bytemap")
			fmt.Println(err2)
			return nil
		}

		fmt.Println("rule handler")
		handler := &RuleHandler{path: name, store: store, bytemap: bytemap, cachedSequence: nil}
		sequence := handler.Init()
		if sequence != nil {
			handler.cachedSequence = sequence
			rule := adversarylab.Rule{Path: name, Sequence: sequence}
			fmt.Println("Sending rule", name, sequence)
			go sendRule(self.source, rule)
		}
		self.handlers[name] = handler

		return handler
	}
}

func sendRule(source adversarylab.PubsubSource, rule adversarylab.Rule) {
	var value = adversarylab.NamedType{Name: "adversarylab.Rule", Value: rule}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	var h codec.Handle = adversarylab.NamedTypeHandle()

	var enc *codec.Encoder = codec.NewEncoder(bw, h)
	var err error = enc.Encode(value)
	if err != nil {
		fmt.Printf("Error encoding packet: %s", err.Error())
		return
	}

	bw.Flush()

	source <- buff.Bytes()
}

func (self *RuleService) handleUpdates() {
	for update := range self.updates {
		fmt.Println("received update", update.Path)
		name := update.Path
		handler := self.handlers.Load(name)
		if handler != nil {
			result := handler.Handle()
			fmt.Println("Sending rule", name, result)
			if result != nil {
				if handler.cachedSequence == nil {
					sendRule(self.source, adversarylab.Rule{Path: name, Sequence: result})
					handler.cachedSequence = result
				} else {
					if !bytes.Equal(result, handler.cachedSequence) {
						sendRule(self.source, adversarylab.Rule{Path: name, Sequence: result})
						handler.cachedSequence = result
					} else {
						fmt.Println("Rejecting duplicate rule", handler.cachedSequence)
					}
				}
			}
		} else {
			fmt.Println("Could not load handler for", name)
		}
	}
}

// Init process all items that are already in storage
func (self *RuleHandler) Init() []byte {
	return self.bytemap.Extract()
}

// Handle handles requests
func (self *RuleHandler) Handle() []byte {
	index := self.bytemap.GetIndex()
	self.store.BlockingFromIndexDo(index, func(record *freefall.Record) {
		self.bytemap.ProcessBytes(record)
	})

	return self.bytemap.Extract()
}

// Save saves StoreData to storage
func (self *RuleHandler) Save() {
	self.bytemap.Save()
}
