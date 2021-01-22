package bloom

import (
	"encoding/json"
	"math"
	"testing"

	btcsuiteWire "github.com/btcsuite/btcd/wire"

	streakKnife "github.com/steakknife/bloomfilter"
	"github.com/willf/bitset"
)

const MaxBitSet = (1 * 1024 * 1024) * 8

func TestWillfFilterSize(t *testing.T) {
	var maxN uint64 = 10000000
	var p float64 = 0.1
	m := uint(streakKnife.OptimalM(maxN, p))
	wordsNeeded := WordsNeeded(m)
	msize := wordsNeeded * 8
	f, _ := NewWillfFilter(maxN, p, MaxBitSet)
	j, _ := f.MarshalJSON()
	type bloomFilterJSON struct {
		M uint           `json:"m"`
		K uint           `json:"k"`
		B *bitset.BitSet `json:"b"`
	}
	bf := &bloomFilterJSON{}
	_ = json.Unmarshal(j, &bf)
	if wordsNeeded != 748833 || msize != 5990664 {
		t.Fatal("size calculation failed")
	}
	if wordsNeeded != len(bf.B.Bytes()) {
		t.Fatal("size calculation failed")
	}

	_ = f.Add([]byte("hello"))
	if !f.Check([]byte("hello")) {
		t.Fatal("check failed")
	}
	if f.Check([]byte("bye")) {
		t.Fatal("check failed")
	}
}

func TestSteakKnifeFilterSize(t *testing.T) {
	var maxN uint64 = 10000
	var p float64 = 0.1
	m := streakKnife.OptimalM(maxN, p)
	k := streakKnife.OptimalK(m, maxN)
	msize := ((m + 63) / 64) * 8
	msize += k * 8
	f, _ := NewSteakKnifeFilter(maxN, p, MaxBitSet)
	j, _ := f.MarshalJSON()
	sk := &SteakKnifeJSON{}
	_ = json.Unmarshal(j, sk)
	if uint64(len(sk.Keys)) != k {
		t.Fatal("size calculation failed")
	}
	if (uint64(len(sk.Bits))+uint64(len(sk.Keys)))*8 != msize {
		t.Fatal("size calculation failed")
	}

	_ = f.Add([]byte("hello"))
	if !f.Check([]byte("hello")) {
		t.Fatal("check failed")
	}
	if f.Check([]byte("bye")) {
		t.Fatal("check failed")
	}
}

func TestBtcsuiteilterSize(t *testing.T) {
	var maxN uint64 = 10000
	var p float64 = 0.1

	dataLen := uint32(-1 * float64(maxN) * math.Log(p) / Ln2Squared)
	dataLen = MinUint32(dataLen, btcsuiteWire.MaxFilterLoadFilterSize*8) / 8

	hashFuncs := uint32(float64(dataLen*8) / float64(maxN) * math.Ln2)
	hashFuncs = MinUint32(hashFuncs, btcsuiteWire.MaxFilterLoadHashFuncs)

	f, _ := NewBtcsuiteFilter(maxN, p, MaxBitSet)
	j, _ := f.MarshalJSON()
	mfl := &MsgFilterLoadJSON{}
	_ = json.Unmarshal(j, mfl)

	if uint32(len(mfl.Filter)) != dataLen {
		t.Fatal("size calculation failed")
	}
	if hashFuncs != mfl.HashFuncs {
		t.Fatal("size calculation failed")
	}

	_ = f.Add([]byte("hello"))
	if !f.Check([]byte("hello")) {
		t.Fatal("check failed")
	}
	if f.Check([]byte("bye")) {
		t.Fatal("check failed")
	}
}
