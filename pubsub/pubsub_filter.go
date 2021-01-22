package pubsub

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/utils/bloom"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/formatting"
	avalancheGoJson "github.com/ava-labs/avalanchego/utils/json"
)

// MaxBitSet the max number of bytes
const MaxBitSet = (1 * 1024 * 1024) * 8

type FilterParam struct {
	lock          sync.RWMutex
	address       map[ids.ShortID]struct{}
	addressFilter bloom.Filter
}

func (f *FilterParam) AddressFiter() bloom.Filter {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.addressFilter
}

func (f *FilterParam) SetAddressFilter(filter bloom.Filter) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.addressFilter = filter
}

func (f *FilterParam) CheckAddress(addr2check []byte) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if f.addressFilter != nil && f.addressFilter.Check(addr2check) {
		return true
	}
	for addr := range f.address {
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
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.addressFilter != nil || len(f.address) > 0
}

func (f *FilterParam) UpdateAddressMulti(unsubscribe bool, max int, bl ...[]byte) error {
	for _, b := range bl {
		address := ByteToID(b)
		err := f.UpdateAddress(unsubscribe, max, address)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FilterParam) UpdateAddress(unsubscribe bool, max int, address ids.ShortID) error {
	switch unsubscribe {
	case true:
		f.lock.Lock()
		delete(f.address, address)
		f.lock.Unlock()
	default:
		lenAddr := 0
		f.lock.RLock()
		lenAddr = len(f.address)
		f.lock.RUnlock()
		if lenAddr > max {
			return fmt.Errorf("address update err max addresses")
		}
		f.lock.Lock()
		f.address[address] = struct{}{}
		f.lock.Unlock()
	}
	return nil
}

func NewFilterParam() *FilterParam {
	return &FilterParam{address: make(map[ids.ShortID]struct{})}
}

const (
	CommandFilterUpdate  = "filterUpdate"
	CommandAddressUpdate = "addressUpdate"

	ParamAddress = "address"

	MaxAddresses = 10000

	DefaultFilterMax   = 1000
	DefaultFilterError = .1
)

// CommandMessage command message
type CommandMessage struct {
	Command       string   `json:"command"`
	AddressUpdate [][]byte `json:"addressUpdate,omitempty"`
	FilterMax     uint64   `json:"filterMax,omitempty"`
	FilterError   float64  `json:"filterError,omitempty"`
	avalancheGoJson.Subscribe
}

func (c *CommandMessage) IsNewFilter() bool {
	return c.FilterMax > 0 && c.FilterError > 0
}

func (c *CommandMessage) FilterOrDefault() {
	if c.IsNewFilter() {
		return
	}
	c.FilterMax = DefaultFilterMax
	c.FilterError = DefaultFilterError
}

// TransposeAddress converts any b32 address to their byte equiv ids.ShortID.
func (c *CommandMessage) TransposeAddress(hrp string) {
	for icnt, a := range c.AddressUpdate {
		astr := string(a)
		// remove chain prefix if found..  X-fuji....
		addressParts := strings.SplitN(astr, "-", 2)
		if len(addressParts) >= 2 {
			astr = addressParts[1]
		}
		if strings.HasPrefix(astr, hrp) && len(astr) > len(ids.ShortEmpty) {
			_, _, abytes, err := formatting.ParseAddress("X-" + astr)
			if err != nil {
				continue
			}
			c.AddressUpdate[icnt] = abytes
		}
	}
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
	_, r, err := c.NextReader()
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
	cmdMsg.TransposeAddress(ps.hrp)
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
	channelBytes, err := json.Marshal(&cmdMsg.Subscribe)
	// unexpected...
	if err != nil {
		errmsg := &errorMsg{Error: fmt.Sprintf("command '%s' err %v", cmdMsg.Command, err)}
		send <- errmsg
		return true, b, fmt.Errorf(errmsg.Error)
	}
	return false, channelBytes, nil
}

func (ps *pubsubfilter) handleCommandFilterUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	bfilter, err := ps.updateNewFilter(cmdMsg, fp)
	if err != nil {
		errmsg := &errorMsg{Error: fmt.Sprintf("filter create failed %v", err)}
		send <- errmsg
		return true, b, nil
	}
	bfilter.Add(cmdMsg.AddressUpdate...)
	return true, b, nil
}

func (ps *pubsubfilter) updateNewFilter(cmdMsg *CommandMessage, fp *FilterParam) (bloom.Filter, error) {
	bfilter := fp.AddressFiter()
	// no filter exists..  Or they provided filter params
	if bfilter == nil || cmdMsg.IsNewFilter() {
		cmdMsg.FilterOrDefault()
		var err error
		bfilter, err = bloom.New(cmdMsg.FilterMax, cmdMsg.FilterError, MaxBitSet)
		if err != nil {
			return nil, err
		}
		fp.SetAddressFilter(bfilter)
	}
	return bfilter, nil
}

func (ps *pubsubfilter) handleCommandAddressUpdate(cmdMsg *CommandMessage, send chan interface{}, fp *FilterParam, b []byte) (bool, []byte, error) {
	err := fp.UpdateAddressMulti(cmdMsg.Unsubscribe, MaxAddresses, cmdMsg.AddressUpdate...)
	if err != nil && send != nil {
		errmsg := &errorMsg{Error: err.Error()}
		send <- errmsg
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
	return ps.queryToFilter(r, NewFilterParam())
}

func (ps *pubsubfilter) queryToFilter(r *http.Request, fp *FilterParam) *FilterParam {
	cmdMsg := &CommandMessage{}
	cmdMsg.AddressUpdate = make([][]byte, 0, 100)
	cmdMsg.Unsubscribe = false
	for valuesk, valuesv := range r.URL.Query() {
		switch valuesk {
		case ParamAddress:
			for _, value := range valuesv {
				// 0x or 0X followed by enough bytes for a ids.ShortID
				if (strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X")) && len(value) == (len(ids.ShortEmpty)+1)*2 {
					sid, err := AddressToID(value[2:])
					if err != nil {
						cmdMsg.AddressUpdate = append(cmdMsg.AddressUpdate, sid[:])
						continue
					}
				}
				//  enough bytes for a ids.ShortID
				if len(value) == len(ids.ShortEmpty)*2 {
					sid, err := AddressToID(value)
					if err != nil {
						cmdMsg.AddressUpdate = append(cmdMsg.AddressUpdate, sid[:])
						continue
					}
				}
				cmdMsg.AddressUpdate = append(cmdMsg.AddressUpdate, []byte(value))
			}
		default:
		}
	}
	cmdMsg.TransposeAddress(ps.hrp)
	_, _, _ = ps.handleCommandAddressUpdate(cmdMsg, nil, fp, []byte(""))
	return fp
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

func AddressToID(address string) (ids.ShortID, error) {
	addrBytes, err := hex.DecodeString(address)
	if err != nil {
		return ids.ShortEmpty, err
	}
	var sid ids.ShortID
	copy(sid[:], addrBytes)
	return sid, nil
}

func ByteToID(address []byte) ids.ShortID {
	var sid ids.ShortID
	copy(sid[:], address)
	return sid
}
