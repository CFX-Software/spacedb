package server

import (
	"encoding/json"
	"testing"
)

func TestTransportRequestProfileFlagUnmarshals(t *testing.T) {
	body := []byte(`{"id":"abc","op":"execute","query":"SELECT 1","profile":true}`)
	var req transportRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.ID != "abc" || req.Op != "execute" || req.Query != "SELECT 1" {
		t.Fatalf("basic fields wrong: %+v", req)
	}
	if !req.Profile {
		t.Fatal("expected Profile=true")
	}
}

func TestTransportRequestProfileDefaultsFalse(t *testing.T) {
	body := []byte(`{"id":"abc","op":"execute"}`)
	var req transportRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Profile {
		t.Fatal("expected Profile=false when omitted")
	}
}

func TestTransportResponseOmitsProfileWhenNil(t *testing.T) {
	resp := transportResponse{ID: "abc", OK: true, Result: 42}
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(encoded)
	want := `{"id":"abc","ok":true,"result":42}`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestTransportResponseIncludesProfileWhenSet(t *testing.T) {
	resp := transportResponse{
		ID: "abc", OK: true, Result: 1,
		Profile: &transportProfile{ServerTotalNs: 100, DispatchNs: 80, DbDurNs: 70},
	}
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundTrip map[string]interface{}
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("round trip unmarshal: %v", err)
	}
	profile, ok := roundTrip["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("profile missing in response: %s", encoded)
	}
	if profile["serverTotalNs"].(float64) != 100 {
		t.Fatalf("serverTotalNs wrong: %v", profile["serverTotalNs"])
	}
	if profile["dispatchNs"].(float64) != 80 {
		t.Fatalf("dispatchNs wrong: %v", profile["dispatchNs"])
	}
	if profile["dbDurNs"].(float64) != 70 {
		t.Fatalf("dbDurNs wrong: %v", profile["dbDurNs"])
	}
}
