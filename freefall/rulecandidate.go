package freefall

import "math"

type RuleCandidate struct {
	Index      int64
	AllowCount int64
	AllowTotal int64
	BlockCount int64
	BlockTotal int64
}

func (self *RuleCandidate) BetterThan(other *RuleCandidate) bool {
	return self.Score() > other.Score()
}

func (self *RuleCandidate) Score() float64 {
	return math.Abs(self.rawScore())
}

func (self *RuleCandidate) Rule() (bool, int64) {
	return self.rawScore() > 0, self.Index
}

func (self *RuleCandidate) rawScore() float64 {
	allow := float64(self.AllowCount) / float64(self.AllowTotal)
	block := float64(self.BlockCount) / float64(self.BlockTotal)
	return allow - block
}
