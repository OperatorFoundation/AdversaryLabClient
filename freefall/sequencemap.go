package freefall

import (
	"fmt"

	"github.com/Workiva/go-datastructures/trie/ctrie"
)

type SequenceMap struct {
	store   *Store
	ctrie   *ctrie.Ctrie
	bytemap *Countmap
}

func NewSequenceMap(name string, updates chan *RuleCandidate) (*SequenceMap, error) {
	store, err := OpenStore(name + "-sequence")
	if err != nil {
		return nil, err
	}
	var ctrie *ctrie.Ctrie = ctrie.New(nil)
	var bytemap *Countmap
	bytemap, err = NewCountmap(name+"-sequence", updates)
	if err != nil {
		return nil, err
	}

	store.BlockingFromIndexDo(0, func(record *Record) {
		ctrie.Insert(record.Data, record)
	})

	return &SequenceMap{store: store, ctrie: ctrie, bytemap: bytemap}, nil
}

func (self *SequenceMap) Increment(allowBlock bool, sequence []byte) {
	//	fmt.Println("Incrementing", len(sequence), sequence)
	if val, ok := self.ctrie.Lookup(sequence); ok {
		// Existing sequence
		record := val.(*Record)
		self.bytemap.IncrementCount(record.Index, allowBlock)
	} else {
		// New sequence
		index := self.store.Add(sequence)
		if index == -1 {
			fmt.Println("Error adding sequence to store", len(sequence), sequence)
			return
		}
		//		fmt.Println("Added sequence", self.store.Path, len(sequence), "got index", index)
		record, err := self.store.GetRecord(index)
		if err != nil {
			fmt.Println("Error adding record")
			return
		}
		if len(record.Data) == 0 {
			fmt.Println("Error, added sequence now has 0 length")
			return
		}
		if record.Index != index {
			fmt.Println("Error, record has incorrect index")
			return
		}

		self.bytemap.IncrementCount(index, allowBlock)
		self.ctrie.Insert(sequence, record)
	}
}

func (self *SequenceMap) ProcessBytes(allowBlock bool, sequence []byte) {
	for length := 1; length <= len(sequence); length++ {
		for offset := 0; offset+length <= len(sequence); offset++ {
			self.Increment(allowBlock, sequence[offset:offset+length])
		}
	}

	self.bytemap.Save()
}
