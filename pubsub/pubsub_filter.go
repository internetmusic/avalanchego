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
	CommandFilterCreate  = "filterCreate"
	CommandFilterUpdate  = "FilterUpdate"
	CommandAddressUpdate = "addressUpdate"
	ParamAddress         = "address"

	MaxAddresses = 10000
)

// CommandMessage command message
// Channel and Unsubscribe match the format of avalancheGoJson.PubSub, and will become the default pass through to underlying pubsub server
type CommandMessage struct {
	Command          string   `json:"command"`
	Channel          string   `json:"channel,omitempty"`
	AddressUpdate    [][]byte `json:"addressUpdate,omitempty"`
	BloomFilterMax   uint64   `json:"bloomFilterMax,omitempty"`
	BloomFilterError float64  `json:"bloomFilterError,omitempty"`
	Unsubscribe      bool     `json:"unsubscribe,omitempty"`
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

func NewFilterParam() *FilterParam {
	return &FilterParam{Address: make(map[ids.ShortID]struct{})}
}

func (ps *pubsubfilter) connectionCallback(conn *avalancheGoJson.Connection, channel string, add bool) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if add {
		if _, exists := ps.filterParams[conn]; !exists {
			ps.filterParams[conn] = NewFilterParam()
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
		filterParamResponse = NewFilterParam()
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
		return ps.handleCommandAddressUpdate(cmdMsg, send, fp, b)
	case CommandFilterUpdate:
		return ps.handleCommandFilterUpdate(cmdMsg, send, fp, b)
	case CommandFilterCreate:
		return ps.handleCommandFilterCreate(cmdMsg, send, fp, b)
	case "":
		return ps.handleCommandEmpty(cmdMsg, send, b)
	default:
		errmsg := &errorMsg{Error: fmt.Sprintf("command '%s' invalid", cmdMsg.Command)}
		send <- errmsg
		return true, b, fmt.Errorf(errmsg.Error)
	}
}

func (ps *pubsubfilter) handleCommandEmpty(cmdMsg *CommandMessage, send chan interface{}, b []byte) (bool, []byte, error) {
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
}

func (ps *pubsubfilter) handleCommandFilterCreate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
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
}

func (ps *pubsubfilter) handleCommandFilterUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	hasFilter := false
	fp.lock.Lock()
	hasFilter = fp.BFilter != nil
	fp.lock.Unlock()

	// no filter exists... lets just make one up
	if !hasFilter {
		cmdMsg.BloomFilterMax = 512
		cmdMsg.BloomFilterError = .1
		ps.handleCommandFilterCreate(cmdMsg, send, fp, b)
	}

	fp.lock.Lock()
	defer fp.lock.Unlock()
	switch fp.BFilter {
	case nil:
		errmsg := &errorMsg{Error: "filter invalid"}
		send <- errmsg
	default:
		for _, addr := range cmdMsg.AddressUpdate {
			sid, err := ByteToID(addr)
			if err != nil {
				continue
			}
			err = fp.BFilter.Add(sid[:])
			if err != nil {
				errmsg := &errorMsg{Error: fmt.Sprintf("filter add error %v", err)}
				send <- errmsg
			}
		}
	}
	return true, b, nil
}

func (ps *pubsubfilter) handleCommandAddressUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	fp.lock.Lock()
	defer fp.lock.Unlock()
	for _, addr := range cmdMsg.AddressUpdate {
		sid, err := ByteToID(addr)
		if err != nil {
			errmsg := &errorMsg{Error: fmt.Sprintf("address update err %v", err)}
			send <- errmsg
			continue
		}

		switch cmdMsg.Unsubscribe {
		case true:
			delete(fp.Address, *sid)
		default:
			if len(fp.Address) > MaxAddresses {
				errmsg := &errorMsg{Error: "address update err max addresses"}
				send <- errmsg
			} else {
				fp.Address[*sid] = struct{}{}
			}
		}
	}
	return true, b, nil
}

func (ps *pubsubfilter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn := ps.po.ServeHTTP(w, r)
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	ps.filterParams[conn] = ps.buildFilter(r)
}

func (ps *pubsubfilter) buildFilter(r *http.Request) *FilterParam {
	fp := NewFilterParam()
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
