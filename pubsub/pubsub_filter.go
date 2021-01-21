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
	Lock          sync.RWMutex
	Address       map[ids.ShortID]struct{}
	AddressFilter BloomFilter
}

func (f *FilterParam) CheckAddress(addr2check []byte) bool {
	f.Lock.RLock()
	defer f.Lock.RUnlock()
	if f.AddressFilter != nil && f.AddressFilter.Check(addr2check) {
		return true
	}
	for addr := range f.Address {
		if compare(addr, addr2check) {
			return true
		}
	}
	return false
}

func compare(a ids.ShortID, b []byte) bool {
	if len(b) != len(a) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if (a[i] & b[i]) != b[i] {
			return false
		}
	}
	return true
}

func (f *FilterParam) HasFilter() bool {
	f.Lock.RLock()
	defer f.Lock.RUnlock()
	return f.AddressFilter != nil || len(f.Address) > 0
}

func (f *FilterParam) UpdateAddress(address ids.ShortID, add bool, max int) error {
	switch add {
	case true:
		f.Lock.Lock()
		delete(f.Address, address)
		f.Lock.Unlock()
	default:
		lenAddr := 0
		f.Lock.RLock()
		lenAddr = len(f.Address)
		f.Lock.RUnlock()
		if lenAddr > max {
			return fmt.Errorf("address update err max addresses")
		}
		f.Lock.Lock()
		f.Address[address] = struct{}{}
		f.Lock.Unlock()
	}
	return nil
}

const (
	CommandFilterUpdate  = "filterUpdate"
	CommandAddressUpdate = "addressUpdate"

	ParamAddress = "address"

	MaxAddresses = 10000
)

// CommandMessage command message
// Channel and Unsubscribe match the format of avalancheGoJson.PubSub, and will become the default pass through to underlying pubsub server
type CommandMessage struct {
	Command       string   `json:"command"`
	Channel       string   `json:"channel,omitempty"`
	AddressUpdate [][]byte `json:"addressUpdate,omitempty"`
	FilterMax     uint64   `json:"filterMax,omitempty"`
	FilterError   float64  `json:"filterError,omitempty"`
	Unsubscribe   bool     `json:"unsubscribe,omitempty"`
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
	var filterParamResponse *FilterParam
	ps.lock.RLock()
	if filterParam, ok := ps.filterParams[c]; ok {
		filterParamResponse = filterParam
	}
	ps.lock.RUnlock()

	if filterParamResponse != nil {
		return filterParamResponse
	}

	ps.lock.Lock()
	defer ps.lock.Unlock()
	if filterParam, ok := ps.filterParams[c]; ok {
		return filterParam
	}
	filterParamResponse = NewFilterParam()
	ps.filterParams[c] = filterParamResponse
	return filterParamResponse
}

func (ps *pubsubfilter) handleCommand(
	cmdMsg *CommandMessage,
	send chan interface{},
	fp *FilterParam,
	b []byte,
) (bool, []byte, error) {
	switch cmdMsg.Command {
	case "":
		return ps.handleCommandEmpty(cmdMsg, send, b)
	case CommandFilterUpdate:
		return ps.handleCommandFilterUpdate(cmdMsg, send, fp, b)
	case CommandAddressUpdate:
		return ps.handleCommandAddressUpdate(cmdMsg, send, fp, b)
	default:
		errmsg := &errorMsg{Error: fmt.Sprintf("command '%s' invalid", cmdMsg.Command)}
		send <- errmsg
		return true, b, fmt.Errorf(errmsg.Error)
	}
}

func (ps *pubsubfilter) handleCommandEmpty(cmdMsg *CommandMessage, send chan interface{}, b []byte) (bool, []byte, error) {
	// re-build this message as avalancheGoJson.Subscribe
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

func (ps *pubsubfilter) handleCommandFilterUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	var bFilter BloomFilter
	fp.Lock.RLock()
	bFilter = fp.AddressFilter
	fp.Lock.RUnlock()

	// no filter exists..  Or they provided filter params
	if bFilter == nil || (cmdMsg.FilterMax > 0 && cmdMsg.FilterError > 0) {
		// filter params not specified.. set defaults
		if !(cmdMsg.FilterMax > 0 && cmdMsg.FilterError > 0) {
			cmdMsg.FilterMax = 1000
			cmdMsg.FilterError = .1
		}
		bfilter, err := NewBloomFilter(cmdMsg.FilterMax, cmdMsg.FilterError)
		if err == nil {
			fp.Lock.Lock()
			fp.AddressFilter = bfilter
			fp.Lock.Unlock()
		} else {
			errmsg := &errorMsg{Error: fmt.Sprintf("filter add error %v", err)}
			send <- errmsg
		}
	}

	fp.Lock.RLock()
	bFilter = fp.AddressFilter
	fp.Lock.RUnlock()

	switch bFilter {
	case nil:
		errmsg := &errorMsg{Error: "filter invalid"}
		send <- errmsg
	default:
		for _, addr := range cmdMsg.AddressUpdate {
			sid := ByteToID(addr)
			err := bFilter.Add(sid[:])
			if err != nil {
				errmsg := &errorMsg{Error: fmt.Sprintf("filter add error %v", err)}
				send <- errmsg
			}
		}
	}
	return true, b, nil
}

func (ps *pubsubfilter) handleCommandAddressUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	for _, addr := range cmdMsg.AddressUpdate {
		err := fp.UpdateAddress(ByteToID(addr), cmdMsg.Unsubscribe, MaxAddresses)
		if err != nil {
			errmsg := &errorMsg{Error: err.Error()}
			send <- errmsg
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
	for valuesk, valuesv := range r.URL.Query() {
		switch valuesk {
		case ParamAddress:
			for _, value := range valuesv {
				_ = fp.UpdateAddress(AddressToID(value), false, MaxAddresses)
			}
		default:
		}
	}
}

func (ps *pubsubfilter) doPublish(channel string, msg interface{}, parser Parser) bool {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	conns, exists := ps.channelMap[channel]
	if !exists {
		return false
	}
	for conn := range conns {
		if fp, exists := ps.filterParams[conn]; exists && fp.HasFilter() {
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

func (ps *pubsubfilter) Publish(channel string, msg interface{}, parser Parser) {
	if ps.doPublish(channel, msg, parser) {
		return
	}
	ps.po.Publish(channel, msg)
}

func (ps *pubsubfilter) Register(channel string) error {
	return ps.po.Register(channel)
}

func AddressToID(address string) ids.ShortID {
	addrBytes, _ := hex.DecodeString(address)
	var sid ids.ShortID
	copy(sid[:], addrBytes)
	return sid
}

func ByteToID(address []byte) ids.ShortID {
	var sid ids.ShortID
	copy(sid[:], address)
	return sid
}
