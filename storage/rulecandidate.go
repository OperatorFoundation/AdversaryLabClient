package storage

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

func (self *RuleCandidate) RequireForbid() bool {
	return self.rawScore() > 0
}

func (self *RuleCandidate) rawScore() float64 {
	if self.AllowTotal < 3 || self.BlockTotal < 3 {
		return 0
	}

	allow := float64(self.AllowCount) / float64(self.AllowTotal)
	block := float64(self.BlockCount) / float64(self.BlockTotal)
	return allow - block
}
