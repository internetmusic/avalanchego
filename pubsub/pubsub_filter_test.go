package pubsub

import (
	"encoding/hex"
	"net/http"
	"net/url"
	"testing"

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

	_ = mockFilter.Add([]byte("hello"))
	if !fp.CheckAddress([]byte("hello")) {
		t.Fatalf("check address failed")
	}
	if fp.CheckAddress([]byte("bye")) {
		t.Fatalf("check address failed")
	}
	idsv = ids.GenerateTestShortID()
	if fp.UpdateAddress(idsv, false, 10) != nil {
		t.Fatalf("update address failed")
	}
	if len(fp.address) != 1 {
		t.Fatalf("update address failed")
	}
	if fp.UpdateAddress(idsv, true, 10) != nil {
		t.Fatalf("update address failed")
	}
	if len(fp.address) != 0 {
		t.Fatalf("update address failed")
	}

	for i := 0; i < 11; i++ {
		idsv := ids.GenerateTestShortID()
		if fp.UpdateAddress(idsv, false, 10) != nil {
			t.Fatalf("update address failed")
		}
	}
	idsv = ids.GenerateTestShortID()
	if fp.UpdateAddress(idsv, false, 10) == nil {
		t.Fatalf("update address failed")
	}
}
