package pubsub

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/formatting"

	avalancheGoJson "github.com/ava-labs/avalanchego/utils/json"

	"github.com/ava-labs/avalanchego/utils/bloom"

	"github.com/ava-labs/avalanchego/ids"
)

func hex2Short(v string) (ids.ShortID, error) {
	bytes, err := hex.DecodeString(v)
	if err != nil {
		return ids.ShortEmpty, err
	}
	idsid, err := ids.ToShortID(bytes)
	if err != nil {
		return ids.ShortEmpty, err
	}
	return idsid, nil
}

func TestFilter(t *testing.T) {
	idaddr1 := "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
	idaddr2 := "0000000000000000000000000000000000000001"

	pubsubfilter := &pubsubfilter{}
	r := http.Request{}
	r.URL = &url.URL{
		RawQuery: ParamAddress + "=" + idaddr1 + "&" +
			ParamAddress + "=" + idaddr2 + "",
	}
	fp := pubsubfilter.buildFilter(&r)
	if len(fp.address) != 2 {
		t.Fatalf("build filter failed")
	}
	ids1, _ := hex2Short(idaddr1)
	if fp.address[ids1] != struct{}{} {
		t.Fatalf("build filter failed %s", "0x"+idaddr1)
	}
	ids2, _ := hex2Short(idaddr2)
	if fp.address[ids2] != struct{}{} {
		t.Fatalf("build filter failed %s", idaddr2)
	}
}

func TestCommandMessage_TransposeAddress(t *testing.T) {
	hrp := constants.GetHRP(5)
	cmdMsg := &CommandMessage{}
	cmdMsg.AddressUpdate = make([][]byte, 0, 1)
	idsid1, _ := hex2Short("0000000000000000000000000000000000000001")
	b32addr, _ := formatting.FormatBech32(hrp, idsid1[:])
	cmdMsg.AddressUpdate = append(cmdMsg.AddressUpdate, []byte("Z-"+b32addr))
	cmdMsg.TransposeAddress(hrp)
	if !bytes.Equal(cmdMsg.AddressUpdate[0], idsid1[:]) {
		t.Fatalf("address transpose failed")
	}
}

func TestCompare(t *testing.T) {
	idsid1, _ := hex2Short("0000000000000000000000000000000000000001")
	idsid2, _ := hex2Short("0000000000000000000000000000000000000001")

	if !compare(idsid1, idsid2[:]) {
		t.Fatalf("filter failed")
	}

	idsid3, _ := hex2Short("0000000000000000000000000000000000000010")

	if compare(idsid1, idsid3[:]) {
		t.Fatalf("filter failed")
	}

	idsid1, _ = hex2Short("0000000000000000000000000000000000000011")
	idsid4, _ := hex2Short("0000000000000000000000000000000000000001")

	if !compare(idsid1, idsid4[:]) {
		t.Fatalf("filter failed")
	}

	idsid1, _ = hex2Short("0000000000000000000000000000000000000011")
	idsid4, _ = hex2Short("0000000000000000000000000000000000000010")

	if !compare(idsid1, idsid4[:]) {
		t.Fatalf("filter failed")
	}
}

func TestFilterParamUpdateMulti(t *testing.T) {
	fp := NewFilterParam()
	bl := make([][]byte, 0, 10)
	bl = append(bl, []byte("abc"))
	bl = append(bl, []byte("def"))
	bl = append(bl, []byte("xyz"))
	_ = fp.UpdateAddressMulti(false, 10, bl...)
	if len(fp.address) != 3 {
		t.Fatalf("update multi failed")
	}
	if _, exists := fp.address[ByteToID([]byte("abc"))]; !exists {
		t.Fatalf("update multi failed")
	}
	if _, exists := fp.address[ByteToID([]byte("def"))]; !exists {
		t.Fatalf("update multi failed")
	}
	if _, exists := fp.address[ByteToID([]byte("xyz"))]; !exists {
		t.Fatalf("update multi failed")
	}
}

func TestFilterParam(t *testing.T) {
	mockFilter := bloom.NewMock()

	fp := NewFilterParam()
	fp.addressFilter = mockFilter

	if !fp.HasFilter() {
		t.Fatalf("has filter failed")
	}

	idsv := ids.GenerateTestShortID()
	fp.address[idsv] = struct{}{}
	if !fp.CheckAddress(idsv[:]) {
		t.Fatalf("check address failed")
	}
	delete(fp.address, idsv)

	mockFilter.Add([]byte("hello"))
	if !fp.CheckAddress([]byte("hello")) {
		t.Fatalf("check address failed")
	}
	if fp.CheckAddress([]byte("bye")) {
		t.Fatalf("check address failed")
	}
	idsv = ids.GenerateTestShortID()
	if fp.UpdateAddress(false, 10, idsv) != nil {
		t.Fatalf("update address failed")
	}
	if len(fp.address) != 1 {
		t.Fatalf("update address failed")
	}
	if fp.UpdateAddress(true, 10, idsv) != nil {
		t.Fatalf("update address failed")
	}
	if len(fp.address) != 0 {
		t.Fatalf("update address failed")
	}

	for i := 0; i < 11; i++ {
		idsv := ids.GenerateTestShortID()
		if fp.UpdateAddress(false, 10, idsv) != nil {
			t.Fatalf("update address failed")
		}
	}
	idsv = ids.GenerateTestShortID()
	if fp.UpdateAddress(false, 10, idsv) == nil {
		t.Fatalf("update address failed")
	}
}

func TestCommandMessage(t *testing.T) {
	cm := &CommandMessage{}
	if cm.IsNewFilter() {
		t.Fatalf("new filter check failed")
	}
	cm.FilterOrDefault()
	if cm.FilterMax != DefaultFilterMax && cm.FilterError != DefaultFilterError {
		t.Fatalf("default filter check failed")
	}
	cm.FilterMax = 1
	cm.FilterError = .1
	cm.FilterOrDefault()
	if cm.FilterMax != 1 && cm.FilterError != .1 {
		t.Fatalf("default filter check failed")
	}
}

func TestFuncCommandEmpty(t *testing.T) {
	psf := &pubsubfilter{}
	send := make(chan interface{}, 100)
	cmdMsg := &CommandMessage{}
	cmdMsg.Channel = "testchannel"
	cmdMsg.Unsubscribe = true
	bin := []byte("testbytes")
	res, bout, err := psf.handleCommandEmpty(cmdMsg, send, bin)
	if err != nil {
		t.Fatalf("handle command empty failed")
	}
	if res {
		t.Fatalf("handle command empty failed")
	}
	channelBytes, _ := json.Marshal(&cmdMsg.Subscribe)
	if !bytes.Equal(bout, channelBytes) {
		t.Fatalf("handle command empty failed")
	}
	if !strings.Contains(string(channelBytes), "\"testchannel\"") {
		t.Fatalf("handle command empty failed")
	}
}

func TestUpdateNewFiler(t *testing.T) {
	psf := &pubsubfilter{}
	cmdMsg := &CommandMessage{}
	fp := NewFilterParam()
	filter, err := psf.updateNewFilter(cmdMsg, fp)
	if err != nil {
		t.Fatalf("handle new filter failed")
	}
	if filter == nil {
		t.Fatalf("handle new filter failed")
	}
}

func TestFetchFilterCommand(t *testing.T) {
	psf := &pubsubfilter{
		filterParams: make(map[*avalancheGoJson.Connection]*FilterParam),
	}
	c := &avalancheGoJson.Connection{}
	fp := psf.fetchFilterParam(c)
	if fp == nil {
		t.Fatalf("fetch filter param failed")
	}
	fp.address = make(map[ids.ShortID]struct{})
	fp.address[ids.ShortEmpty] = struct{}{}
	fp = psf.fetchFilterParam(c)
	if fp == nil {
		t.Fatalf("fetch filter param failed")
	}
	if len(fp.address) == 0 {
		t.Fatalf("fetch filter param failed")
	}
	if _, exists := fp.address[ids.ShortEmpty]; !exists {
		t.Fatalf("fetch filter param failed")
	}
}
