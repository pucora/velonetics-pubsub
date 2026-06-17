package kafka

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func TestCapAPIVersionsForAzure(t *testing.T) {
	versions := map[protocol.ApiKey]int16{
		protocol.SaslHandshake:    1,
		protocol.SaslAuthenticate: 1,
		protocol.Metadata:         9,
	}
	capped := capAPIVersionsForAzure(versions)
	if capped[protocol.SaslHandshake] != 0 {
		t.Fatalf("expected SaslHandshake v0, got %d", capped[protocol.SaslHandshake])
	}
	if _, ok := capped[protocol.SaslAuthenticate]; ok {
		t.Fatal("expected SaslAuthenticate to be removed for Azure Event Hub")
	}
	if capped[protocol.Metadata] != 9 {
		t.Fatalf("expected other API versions unchanged, got %d", capped[protocol.Metadata])
	}
}

func TestSASLSendHandshakeDefault(t *testing.T) {
	var cfg saslCfg
	if !cfg.sendHandshake() {
		t.Fatal("expected handshake enabled by default")
	}
}

func TestSASLSkipHandshakeWhenDisabled(t *testing.T) {
	disabled := false
	cfg := saslCfg{DisableHanshake: &disabled}
	if cfg.sendHandshake() {
		t.Fatal("expected handshake disabled when disable_hanshake is false")
	}
	if !cfg.needsCustomConnect() {
		t.Fatal("expected custom connect when handshake is skipped")
	}
}

func TestBuildSASLMechanismOAuthBearer(t *testing.T) {
	mech, err := buildSASLMechanism(saslCfg{
		Mechanism: "OAUTHBEARER",
		Password:  "access-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mech.Name() != "OAUTHBEARER" {
		t.Fatalf("unexpected mechanism: %s", mech.Name())
	}
	_, state, err := mech.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state) == 0 {
		t.Fatal("expected non-empty initial SASL state")
	}
}

func TestBuildSASLMechanismPlainAzureDefaultUser(t *testing.T) {
	mech, err := buildSASLMechanism(saslCfg{
		AzureEventHub: true,
		Password:      "Endpoint=sb://...",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, state, err := mech.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := "\x00$ConnectionString\x00Endpoint=sb://..."
	if string(state) != want {
		t.Fatalf("unexpected PLAIN payload: %q", string(state))
	}
}

func TestBuildSASLMechanismPlainAuthIdentity(t *testing.T) {
	mech, err := buildSASLMechanism(saslCfg{
		AuthIdentity: "authz",
		User:         "user",
		Password:     "pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, state, err := mech.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := "\x00authz\x00user\x00pass"
	if string(state) != want {
		t.Fatalf("unexpected PLAIN payload: %q", string(state))
	}
}

func TestBuildSASLMechanismUnsupported(t *testing.T) {
	_, err := buildSASLMechanism(saslCfg{
		Mechanism: "SCRAM-SHA-512",
		User:      "u",
		Password:  "p",
	})
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}
