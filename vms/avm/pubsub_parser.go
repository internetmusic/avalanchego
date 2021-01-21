package avm

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/pubsub"
	"github.com/ava-labs/avalanchego/vms/components/avax"
)

type parser struct {
	tx *Tx
}

func NewPubSubParser(tx *Tx) pubsub.Parser {
	return &parser{tx: tx}
}

// Apply the filter on the addresses.
// param is unchanged during filtering.
func (p *parser) Filter(param *pubsub.FilterParam) *pubsub.FilterResponse {
	for _, utxo := range p.tx.UTXOs() {
		switch utxoOut := utxo.Out.(type) {
		case avax.Addressable:
			addresses := utxoOut.Addresses()
			for _, address := range addresses {
				var sid ids.ShortID
				if len(address) != len(sid) {
					continue
				}
				copy(sid[:], address)
				if param.Bfilter != nil {
					matched, err := param.Bfilter.Check(sid[:])
					if err == nil && matched {
						return &pubsub.FilterResponse{TxID: p.tx.ID(), FilteredAddress: sid}
					}
				}
				for addr := range param.Address {
					if compare(addr, sid) {
						return &pubsub.FilterResponse{TxID: p.tx.ID(), FilteredAddress: sid}
					}
				}
			}
		default:
		}
	}
	return nil
}

func compare(a ids.ShortID, b ids.ShortID) bool {
	for i := 0; i < len(a); i++ {
		if (a[i] & b[i]) != b[i] {
			return false
		}
	}
	return true
}
