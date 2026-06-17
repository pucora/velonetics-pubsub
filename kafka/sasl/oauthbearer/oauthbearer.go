// Package oauthbearer implements the OAUTHBEARER SASL mechanism (RFC 7628).
package oauthbearer

import (
	"context"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go/sasl"
)

// TokenFunc returns a bearer access token.
type TokenFunc func(context.Context) (string, error)

// Mechanism implements SASL OAUTHBEARER authentication.
type Mechanism struct {
	TokenFunc TokenFunc
	AuthzID   string
}

func (Mechanism) Name() string {
	return "OAUTHBEARER"
}

func (m Mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	if m.TokenFunc == nil {
		return nil, nil, fmt.Errorf("oauthbearer: token provider is required")
	}
	token, err := m.TokenFunc(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &session{token: token}, initialResponse(m.AuthzID, token), nil
}

type session struct {
	token string
}

func (s *session) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) > 0 {
		// RFC 7628: return dummy response on authentication failure.
		return false, []byte{0x01, 0x00}, nil
	}
	return true, nil, nil
}

func initialResponse(authzID, token string) []byte {
	var b strings.Builder
	b.WriteString("n,")
	if authzID != "" {
		b.WriteByte('a')
		b.WriteString("=")
		b.WriteString(authzID)
	}
	b.WriteString(",")
	b.WriteString("\x01auth=Bearer ")
	b.WriteString(token)
	b.WriteByte('\x01')
	return []byte(b.String())
}

// StaticToken returns a TokenFunc that always returns the same token.
func StaticToken(token string) TokenFunc {
	return func(context.Context) (string, error) {
		if token == "" {
			return "", fmt.Errorf("oauthbearer: access token is required")
		}
		return token, nil
	}
}
