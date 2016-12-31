package services

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/ugorji/go/codec"

	"github.com/OperatorFoundation/AdversaryLab/storage"
	"github.com/OperatorFoundation/AdversaryLab/protocol"
)

type RuleHandlers struct {
	handlers   map[string]*RuleHandler
	source     protocol.PubsubSource
	storeCache *storage.StoreCache
}

// StoreHandler is a request handler that knows about storage
type RuleHandler struct {
	path       string
	store      *storage.Store
	cachedRule *storage.RuleCandidate
}

type RuleService struct {
	handlers RuleHandlers
	serve    protocol.PubsubServer
	updates  chan Update
	source   protocol.PubsubSource
}

func NewRuleService(listenAddress string, updates chan Update, storeCache *storage.StoreCache) *RuleService {
	source := make(protocol.PubsubSource)

	handlers := RuleHandlers{handlers: make(map[string]*RuleHandler), source: source, storeCache: storeCache}
	files, err := ioutil.ReadDir("store")
	if err != nil {
		fmt.Println("Failed to read store directory", err)
	} else {
		for _, file := range files {
			handlers.Load(file.Name())
		}
	}

	serve := protocol.PubsubListen(listenAddress, source)

	return &RuleService{handlers: handlers, serve: serve, updates: updates, source: source}
}

func (self *RuleService) Run() {
	go self.handleUpdates()
	self.serve.Pump()
}

func (self RuleHandlers) Load(name string) *RuleHandler {
	var store *storage.Store
	var err error

	if handler, ok := self.handlers[name]; ok {
		return handler
	} else {
		store = self.storeCache.Get(name + "-offsets-sequence")
		if store == nil {
			store, err = storage.OpenStore(name + "-offsets-sequence")
			if err != nil {
				fmt.Println("Error opening store")
				fmt.Println(err)
				return nil
			}

			self.storeCache.Put(name, store)
		}

		//		fmt.Println("New rule store", store)
		handler := &RuleHandler{path: name, store: store, cachedRule: nil}
		self.handlers[name] = handler

		return handler
	}
}

func sendRule(source protocol.PubsubSource, rule *protocol.Rule) {
	var value = protocol.NamedType{Name: "protocol.Rule", Value: rule}

	var buff = new(bytes.Buffer)
	var bw = bufio.NewWriter(buff)
	var h codec.Handle = protocol.NamedTypeHandle()

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
		//		fmt.Println("received update", update)
		name := update.Path
		handler := self.handlers.Load(name)
		if handler != nil {
			result := handler.Handle(name, update.Rule)
			if result != nil {
				fmt.Println("Sending rule", name, len(result.Sequence), result)
				fmt.Print("!")
				sendRule(self.source, result)
			}
		} else {
			fmt.Println("Could not load handler for", name)
		}
	}
}

// Handle handles requests
func (self *RuleHandler) Handle(name string, cn *storage.RuleCandidate) *protocol.Rule {
	self.cachedRule = cn
	index := cn.Index
	//	fmt.Println("Handle", self.store)
	storage.Debug = true
	record, err := self.store.GetRecord(index)
	storage.Debug = false
	if err != nil {
		return nil
	}

	fmt.Println("Rule record:", record)

	sequence := record.Data
	parts := strings.Split(name, "-")

	return &protocol.Rule{Dataset: parts[0], RequireForbid: cn.RequireForbid(), Incoming: parts[1] == "incoming", Sequence: sequence}
}
