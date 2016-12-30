package freefall

import (
	"github.com/orcaman/concurrent-map"
)

type StoreCache struct {
	cmap.ConcurrentMap
}

func NewStoreCache() *StoreCache {
	return &StoreCache{cmap.New()}
}

func (self *StoreCache) Get(name string) *Store {
	val, ok := self.ConcurrentMap.Get(name)
	if ok {
		return val.(*Store)
	} else {
		return nil
	}
}

func (self *StoreCache) Put(name string, store *Store) {
	self.ConcurrentMap.Set(name, store)
}
