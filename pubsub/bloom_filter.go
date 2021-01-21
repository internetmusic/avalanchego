package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/willf/bitset"

	btcsuiteWire "github.com/btcsuite/btcd/wire"
	btcsuite "github.com/btcsuite/btcutil/bloom"
	"github.com/spaolacci/murmur3"
	streakKnife "github.com/steakknife/bloomfilter"
	willfBloom "github.com/willf/bloom"
)

type FilterType int

var FilterTypeSteakKnife FilterType = 1
var FilterTypeWillf FilterType = 2
var FilterTypeBtcsuite FilterType = 3
var FilterTypeDefault = FilterTypeWillf

const MaxBitSet = 1 * 1024 * 1024

type BloomFilter interface {
	// Add adds to filter, assumed thread safe
	Add([]byte) error
	// Check checks filter, assumed thread safe
	Check([]byte) bool
}

func NewBloomFilter(maxN uint64, p float64) (BloomFilter, error) {
	switch FilterTypeDefault {
	case FilterTypeSteakKnife:
		return NewSteakKnifeFilter(maxN, p)
	case FilterTypeWillf:
		return NewWillfFilter(maxN, p)
	case FilterTypeBtcsuite:
		return NewBtcsuiteFilter(maxN, p)
	}
	return NewWillfFilter(maxN, p)
}

type willfFilter struct {
	lock    sync.RWMutex
	bfilter *willfBloom.BloomFilter
}

func NewWillfFilter(maxN uint64, p float64) (BloomFilter, error) {
	m := uint(streakKnife.OptimalM(maxN, p))
	k := uint(streakKnife.OptimalK(uint64(m), maxN))

	// this is pulled from bitset.
	// the calculation is the size of the bitset which would be created from this filter.
	// to ensure we don't crash memory, we would ensure the size
	// 8 == sizeof(uint64))
	msize := WordsNeeded(m) * 8
	if msize > MaxBitSet {
		return nil, fmt.Errorf("filter too large")
	}
	return &willfFilter{bfilter: willfBloom.New(m, k)}, nil
}

func (f *willfFilter) Add(b []byte) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.bfilter.Add(b)
	return nil
}

func (f *willfFilter) Check(b []byte) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.bfilter.Test(b)
}

type steakKnifeFilter struct {
	lock    sync.RWMutex
	bfilter *streakKnife.Filter
}

func NewSteakKnifeFilter(maxN uint64, p float64) (BloomFilter, error) {
	m := streakKnife.OptimalM(maxN, p)
	k := streakKnife.OptimalK(m, maxN)

	// this is pulled from bloomFilter.newBits and bloomfilter.newRandKeys
	// the calculation is the size of the bitset which would be created from this filter.
	// to ensure we don't crash memory, we would ensure the size
	// 8 == sizeof(uint64))
	msize := ((m + 63) / 64) * 8
	msize += k * 8
	if msize > MaxBitSet {
		return nil, fmt.Errorf("filter too large")
	}
	bfilter, err := streakKnife.New(m, k)
	return &steakKnifeFilter{bfilter: bfilter}, err
}

func (f *steakKnifeFilter) Add(b []byte) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	h := murmur3.New64()
	_, err := h.Write(b)
	if err != nil {
		return err
	}
	f.bfilter.Add(h)
	return nil
}

func (f *steakKnifeFilter) Check(b []byte) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	h := murmur3.New64()
	_, err := h.Write(b)
	if err != nil {
		return false
	}
	return f.bfilter.Contains(h)
}

type btcsuiteFilter struct {
	lock    sync.RWMutex
	bfilter *btcsuite.Filter
}

func NewBtcsuiteFilter(maxN uint64, p float64) (BloomFilter, error) {
	tweak := uint32(time.Now().UnixNano())
	bfilter := btcsuite.NewFilter(uint32(maxN), tweak, p, btcsuiteWire.BloomUpdateNone)
	return &btcsuiteFilter{bfilter: bfilter}, nil
}

func (f *btcsuiteFilter) Add(b []byte) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.bfilter.Add(b)
	return nil
}

func (f *btcsuiteFilter) Check(b []byte) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.bfilter.Matches(b)
}

// the wordSize of a bit set
const wordSize = uint(64)

// log2WordSize is lg(wordSize)
const log2WordSize = uint(6)

func WordsNeeded(i uint) int {
	if i > (bitset.Cap() - wordSize + 1) {
		return int(bitset.Cap() >> log2WordSize)
	}
	return int((i + (wordSize - 1)) >> log2WordSize)
}
