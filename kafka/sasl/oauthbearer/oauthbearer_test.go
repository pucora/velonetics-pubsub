package oauthbearer

import (
	"context"
	"strings"
	"testing"
)

func TestMechanismStart(t *testing.T) {
	m := Mechanism{
		TokenFunc: StaticToken("my-token"),
		AuthzID:   "alice",
	}
	_, state, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := string(state)
	if !strings.Contains(got, "auth=Bearer my-token") {
		t.Fatalf("unexpected initial response: %q", got)
	}
	if !strings.HasPrefix(got, "n,a=alice,") {
		t.Fatalf("expected authzid prefix, got %q", got)
	}
}

func TestMechanismNextOnChallenge(t *testing.T) {
	m := Mechanism{TokenFunc: StaticToken("token")}
	sess, _, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done, resp, err := sess.Next(context.Background(), []byte("error"))
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("expected authentication to fail")
	}
	if len(resp) != 2 || resp[0] != 0x01 || resp[1] != 0x00 {
		t.Fatalf("unexpected failure response: %v", resp)
	}
}

func TestStaticTokenRequired(t *testing.T) {
	m := Mechanism{TokenFunc: StaticToken("")}
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}
