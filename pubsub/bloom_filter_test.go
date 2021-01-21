package pubsub

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"

	btcsuiteWire "github.com/btcsuite/btcd/wire"

	streakKnife "github.com/steakknife/bloomfilter"
	"github.com/willf/bitset"
)

func TestWillfFilterSize(t *testing.T) {
	var maxN uint64 = 10000000
	var p float64 = 0.1
	m := uint(streakKnife.OptimalM(maxN, p))
	wordsNeeded := WordsNeeded(m)
	msize := wordsNeeded * 8
	f, _ := NewWillfFilter(maxN, p)
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

type steakKnifeStruct struct {
	k    uint64
	n    uint64
	m    uint64
	keys []uint64
	bits []uint64
}

func parseSteakKnifeText(byts []byte) (*steakKnifeStruct, error) {
	/*
		k
		4
		n
		0
		m
		48
		keys
		0000000000000000
		0000000000000001
		0000000000000002
		0000000000000003
		bits
		0000000000000000
		sha384
		000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f
	*/
	res := &steakKnifeStruct{}
	scanner := bufio.NewScanner(bytes.NewReader(byts))
	scanner.Buffer(make([]byte, 10e8), 10e8)
	var b string
	if scanner.Scan() {
		b = string(scanner.Bytes())
		if b != "k" {
			return nil, fmt.Errorf("expect k")
		}
		if scanner.Scan() {
			b = string(scanner.Bytes())
			res.k, _ = strconv.ParseUint(b, 10, 64)
		}
	}
	if scanner.Scan() {
		b = string(scanner.Bytes())
		if b != "n" {
			return nil, fmt.Errorf("expect m")
		}
		if scanner.Scan() {
			b = string(scanner.Bytes())
			res.n, _ = strconv.ParseUint(b, 10, 64)
		}
	}
	if scanner.Scan() {
		b = string(scanner.Bytes())
		if b != "m" {
			return nil, fmt.Errorf("expect m")
		}
		if scanner.Scan() {
			b = string(scanner.Bytes())
			res.m, _ = strconv.ParseUint(b, 10, 64)
		}
	}
	if scanner.Scan() {
		b = string(scanner.Bytes())
		if b != "keys" {
			return nil, fmt.Errorf("expect keys")
		}
		for scanner.Scan() {
			b = string(scanner.Bytes())
			if b == "bits" {
				break
			}
			hbits, _ := hex.DecodeString(b)
			if len(hbits) != 8 {
				return nil, fmt.Errorf("invalid hkeys sz")
			}
			num, _ := binary.Uvarint(hbits[0:8])
			res.keys = append(res.keys, num)
		}
	}
	if b != "bits" {
		return nil, fmt.Errorf("expect bits")
	}
	for scanner.Scan() {
		b = string(scanner.Bytes())
		if b == "sha384" {
			break
		}
		hbits, _ := hex.DecodeString(b)
		if len(hbits) != 8 {
			return nil, fmt.Errorf("invalid hbits sz")
		}
		num, _ := binary.Uvarint(hbits[0:8])
		res.bits = append(res.bits, num)
	}
	if b != "sha384" {
		return nil, fmt.Errorf("expect sha384")
	}
	return res, nil
}

func TestSteakKnifeFilterSize(t *testing.T) {
	var maxN uint64 = 10000
	var p float64 = 0.1
	m := streakKnife.OptimalM(maxN, p)
	k := streakKnife.OptimalK(m, maxN)
	msize := ((m + 63) / 64) * 8
	msize += k * 8
	f, _ := NewSteakKnifeFilter(maxN, p)
	j, _ := f.MarshalText()
	sk, _ := parseSteakKnifeText(j)
	if uint64(len(sk.keys)) != k {
		t.Fatal("size calculation failed")
	}
	if (uint64(len(sk.bits))+uint64(len(sk.keys)))*8 != msize {
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

	f, _ := NewBtcsuiteFilter(maxN, p)
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
