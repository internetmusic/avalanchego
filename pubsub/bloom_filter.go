package pubsub

import (
	"encoding/json"
	"fmt"
	"math"
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

// MaxBitSet the max number of bytes
const MaxBitSet = (1 * 1024 * 1024) * 8

type BloomFilter interface {
	// Add adds to filter, assumed thread safe
	Add([]byte) error
	// Check checks filter, assumed thread safe
	Check([]byte) bool
	MarshalJSON() ([]byte, error)
	MarshalText() ([]byte, error)
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
	wordsNeeded := WordsNeeded(m)
	msize := wordsNeeded * 8
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

func (f *willfFilter) MarshalJSON() ([]byte, error) {
	return f.bfilter.MarshalJSON()
}

func (f *willfFilter) MarshalText() ([]byte, error) {
	return []byte(""), fmt.Errorf("unimplemented")
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

func (f *steakKnifeFilter) MarshalJSON() ([]byte, error) {
	return []byte(""), fmt.Errorf("unimplemented")
}

func (f *steakKnifeFilter) MarshalText() ([]byte, error) {
	return f.bfilter.MarshalText()
}

type btcsuiteFilter struct {
	lock    sync.RWMutex
	bfilter *btcsuite.Filter
}

const Ln2Squared = math.Ln2 * math.Ln2

func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func NewBtcsuiteFilter(maxN uint64, p float64) (BloomFilter, error) {
	tweak := uint32(time.Now().UnixNano())

	dataLen := uint32(-1 * float64(maxN) * math.Log(p) / Ln2Squared)
	dataLen = MinUint32(dataLen, btcsuiteWire.MaxFilterLoadFilterSize*8) / 8

	if dataLen > MaxBitSet/8 {
		return nil, fmt.Errorf("filter too large")
	}

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

type MsgFilterLoadJSON struct {
	HashFuncs uint32                       `json:"hashFuncs"`
	Tweak     uint32                       `json:"tweak"`
	Flags     btcsuiteWire.BloomUpdateType `json:"updateType"`
	Filter    []byte                       `json:"filter"`
}

func (f *btcsuiteFilter) MarshalJSON() ([]byte, error) {
	filterLoad := f.bfilter.MsgFilterLoad()
	j := &MsgFilterLoadJSON{
		Filter:    filterLoad.Filter,
		HashFuncs: filterLoad.HashFuncs,
		Tweak:     filterLoad.Tweak,
		Flags:     filterLoad.Flags,
	}
	return json.Marshal(j)
}

func (f *btcsuiteFilter) MarshalText() ([]byte, error) {
	return []byte(""), fmt.Errorf("unimplemented")
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
