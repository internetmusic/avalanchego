package avm

import (
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
			for _, address := range utxoOut.Addresses() {
				response := p.filterParam(param, address)
				if response != nil {
					return response
				}
			}
		default:
		}
	}
	return nil
}

func (p *parser) filterParam(param *pubsub.FilterParam, sid []byte) *pubsub.FilterResponse {
	if param.CheckAddress(sid) {
		return &pubsub.FilterResponse{TxID: p.tx.ID(), FilteredAddress: pubsub.ByteToID(sid)}
	}
	return nil
}
