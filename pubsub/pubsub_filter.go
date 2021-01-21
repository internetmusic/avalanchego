package pubsub

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/formatting"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	avalancheGoJson "github.com/ava-labs/avalanchego/utils/json"
)

type FilterParam struct {
	lock    sync.RWMutex
	Address map[ids.ShortID]struct{}
	BFilter BloomFilter
}

const (
	CommandFilterCreate   = "filterCreate"
	CommandBloomFilterAdd = "bloomFilterAdd"
	CommandAddressUpdate  = "AddressUpdate"
	ParamAddress          = "address"

	MaxAddresses = 10000
)

// CommandMessage command message
// Channel and Unsubscribe match the format of avalancheGoJson.PubSub, and will become the default pass through to underlying pubsub server
type CommandMessage struct {
	Command          string   `json:"command"`
	Channel          string   `json:"channel"`
	AddressUpdate    [][]byte `json:"addressUpdate"`
	BloomFilterMax   uint64   `json:"bloomFilterMax"`
	BloomFilterError float64  `json:"bloomFilterError"`
	Unsubscribe      bool     `json:"unsubscribe"`
}

type errorMsg struct {
	Error string `json:"error"`
}

type FilterResponse struct {
	Channel         string      `json:"channel"`
	TxID            ids.ID      `json:"txID"`
	Address         string      `json:"address"`
	FilteredAddress ids.ShortID `json:"filteredAddress"`
}

type Parser interface {
	// expected a FilterResponse or nil if filter doesn't match
	Filter(*FilterParam) *FilterResponse
}

type Filter interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
	Publish(channel string, msg interface{}, parser Parser)
	Register(channel string) error
}

type pubsubfilter struct {
	hrp          string
	po           *avalancheGoJson.PubSubServer
	lock         sync.RWMutex
	filterParams map[*avalancheGoJson.Connection]*FilterParam
	channelMap   map[string]map[*avalancheGoJson.Connection]struct{}
}

func NewPubSubServerWithFilter(ctx *snow.Context) Filter {
	hrp := constants.GetHRP(ctx.NetworkID)
	po := avalancheGoJson.NewPubSubServer(ctx)
	psf := &pubsubfilter{
		hrp:          hrp,
		po:           po,
		channelMap:   make(map[string]map[*avalancheGoJson.Connection]struct{}),
		filterParams: make(map[*avalancheGoJson.Connection]*FilterParam),
	}
	// inject our callbacks..
	po.SetReadCallback(psf.readCallback, psf.connectionCallback)
	return psf
}

func (ps *pubsubfilter) connectionCallback(conn *avalancheGoJson.Connection, channel string, add bool) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if add {
		if _, exists := ps.filterParams[conn]; !exists {
			ps.filterParams[conn] = &FilterParam{}
		}
		if _, exists := ps.channelMap[channel]; !exists {
			ps.channelMap[channel] = make(map[*avalancheGoJson.Connection]struct{})
		}
		ps.channelMap[channel][conn] = struct{}{}
	} else {
		if channel, exists := ps.channelMap[channel]; exists {
			delete(channel, conn)
		}
		delete(ps.filterParams, conn)
	}
}

func (ps *pubsubfilter) readCallback(c *avalancheGoJson.Connection, send chan interface{}) (bool, []byte, error) {
	var bb bytes.Buffer
	_, r, err := c.Conn.NextReader()
	if err != nil {
		return true, []byte(""), err
	}
	_, err = bb.ReadFrom(r)
	if err != nil {
		return true, []byte(""), err
	}
	b := bb.Bytes()
	cmdMsg := &CommandMessage{}
	err = json.NewDecoder(bytes.NewReader(b)).Decode(cmdMsg)
	if err != nil {
		return true, b, err
	}

	return ps.handleCommand(cmdMsg, send, ps.fetchFilterParam(c), b)
}

func (ps *pubsubfilter) fetchFilterParam(c *avalancheGoJson.Connection) *FilterParam {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	var filterParamResponse *FilterParam
	if filterParam, ok := ps.filterParams[c]; ok {
		filterParamResponse = filterParam
	} else {
		filterParamResponse = &FilterParam{}
		ps.filterParams[c] = filterParamResponse
	}
	return filterParamResponse
}

func (ps *pubsubfilter) handleCommand(
	cmdMsg *CommandMessage,
	send chan interface{},
	fp *FilterParam,
	b []byte,
) (bool, []byte, error) {
	switch cmdMsg.Command {
	case CommandAddressUpdate:
		fp.lock.Lock()
		for _, addr := range cmdMsg.AddressUpdate {
			sid, err := ByteToID(addr)
			if err != nil {
				errmsg := &errorMsg{Error: fmt.Sprintf("address update err %v", err)}
				send <- errmsg
			} else if sid != nil {
				if cmdMsg.Unsubscribe {
					delete(fp.Address, *sid)
				} else {
					if len(fp.Address) > MaxAddresses {
						errmsg := &errorMsg{Error: "address update err max addresses"}
						send <- errmsg
					} else {
						fp.Address[*sid] = struct{}{}
					}
				}
			}
		}
		fp.lock.Unlock()
		return true, b, nil
	case CommandBloomFilterAdd:
		fp.lock.Lock()
		// no filter exists... lets just make one up
		if fp.BFilter == nil {
			bfilter, err := NewBloomFilter(512, .1)
			if err == nil {
				fp.BFilter = bfilter
			} else {
				errmsg := &errorMsg{Error: fmt.Sprintf("filter create error %v", err)}
				send <- errmsg
			}
		}
		if fp.BFilter == nil {
			errmsg := &errorMsg{Error: "filter invalid"}
			send <- errmsg
		} else {
			for _, addr := range cmdMsg.AddressUpdate {
				sid, err := ByteToID(addr)
				if err == nil {
					err = fp.BFilter.Add(sid[:])
					if err != nil {
						errmsg := &errorMsg{Error: fmt.Sprintf("filter add error %v", err)}
						send <- errmsg
					}
				}
			}
		}
		fp.lock.Unlock()
		return true, b, nil
	case CommandFilterCreate:
		bfilter, err := NewBloomFilter(cmdMsg.BloomFilterMax, cmdMsg.BloomFilterError)
		if err == nil {
			fp.lock.Lock()
			fp.BFilter = bfilter
			fp.lock.Unlock()
		} else {
			errmsg := &errorMsg{Error: fmt.Sprintf("filter create error %v", err)}
			send <- errmsg
		}
		return true, b, nil
	case "":
		// default condition re-builds this message as avalancheGoJson.Subscribe
		// and allows parent pubsub_server to handle the request
		channelCommand := &avalancheGoJson.Subscribe{Channel: cmdMsg.Channel, Unsubscribe: cmdMsg.Unsubscribe}
		channelBytes, err := json.Marshal(channelCommand)

		// unexpected...
		if err != nil {
			errmsg := &errorMsg{Error: fmt.Sprintf("command '%s' err %v", cmdMsg.Command, err)}
			send <- errmsg
			return true, b, fmt.Errorf(errmsg.Error)
		}
		return false, channelBytes, nil
	default:
		errmsg := &errorMsg{Error: fmt.Sprintf("command '%s' invalid", cmdMsg.Command)}
		send <- errmsg
		return true, b, fmt.Errorf(errmsg.Error)
	}
}

func (ps *pubsubfilter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn := ps.po.ServeHTTP(w, r)
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	ps.filterParams[conn] = ps.buildFilter(r)
}

func (ps *pubsubfilter) buildFilter(r *http.Request) *FilterParam {
	fp := &FilterParam{}
	ps.queryToFilter(r, fp)
	return fp
}

func (ps *pubsubfilter) queryToFilter(r *http.Request, fp *FilterParam) {
	var values = r.URL.Query()
	for valuesk := range values {
		if valuesk != ParamAddress {
			continue
		}
		for _, value := range values[valuesk] {
			sid, err := AddressToID(value)
			if err == nil {
				if len(fp.Address) <= MaxAddresses {
					fp.Address[*sid] = struct{}{}
				}
			}
		}
	}
}

func (ps *pubsubfilter) doPublish(channel string, msg interface{}, parser Parser) bool {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	if conns, exists := ps.channelMap[channel]; exists {
		for conn := range conns {
			if fp, exists := ps.filterParams[conn]; exists && (fp.BFilter != nil || len(fp.Address) != 0) {
				fr := parser.Filter(fp)
				if fr == nil {
					continue
				}
				fr.Channel = channel
				fr.Address, _ = formatting.FormatBech32(ps.hrp, fr.FilteredAddress.Bytes())
				ps.po.PublishRaw(conn, fr)
			} else {
				m := &avalancheGoJson.Publish{
					Channel: channel,
					Value:   msg,
				}
				ps.po.PublishRaw(conn, m)
			}
		}
		return true
	}
	return false
}

func (ps *pubsubfilter) Publish(channel string, msg interface{}, parser Parser) {
	if ps.doPublish(channel, msg, parser) {
		return
	}
	ps.po.Publish(channel, msg)
}

func (ps *pubsubfilter) Register(channel string) error {
	return ps.po.Register(channel)
}

func AddressToID(address string) (*ids.ShortID, error) {
	addrBytes, err := hex.DecodeString(address)
	if err != nil {
		return nil, err
	}
	lshort := len(ids.ShortEmpty)
	if len(addrBytes) != lshort {
		return nil, fmt.Errorf("address length %d != %d", len(addrBytes), lshort)
	}
	var sid ids.ShortID
	copy(sid[:], addrBytes[:lshort])
	return &sid, nil
}

func ByteToID(address []byte) (*ids.ShortID, error) {
	lshort := len(ids.ShortEmpty)
	if len(address) != lshort {
		return nil, fmt.Errorf("address length %d != %d", len(address), lshort)
	}
	var sid ids.ShortID
	copy(sid[:], address[:lshort])
	return &sid, nil
}
