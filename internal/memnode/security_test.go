package memnode

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/auth"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
)

func TestRecoveryDeniedForClientIdentity(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{Authorizer: &auth.Authorizer{}})
	ctx := auth.WithIdentity(context.Background(), auth.Identity{Name: "c", Role: auth.RoleClient})
	_, err := n.RecoveryForceAbort(ctx, proto.RecoverForceAbortRequest{})
	if err == nil {
		t.Fatalf("expected forbidden")
	}
}

func TestExecPrepareAllowedForClientIdentity(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{Authorizer: &auth.Authorizer{}})
	ctx := auth.WithIdentity(context.Background(), auth.Identity{Name: "c", Role: auth.RoleClient})
	_, err := n.ExecAndPrepare(ctx, proto.ExecPrepareRequest{TID: proto.TransactionID{Epoch: 1}})
	if err != nil {
		t.Fatalf("expected allowed, got %v", err)
	}
}

func TestRateLimitRejectsGracefully(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{PrepareRate: 1, Burst: 1})
	_, err1 := n.ExecAndPrepare(context.Background(), proto.ExecPrepareRequest{TID: proto.TransactionID{Epoch: 1}})
	_, err2 := n.ExecAndPrepare(context.Background(), proto.ExecPrepareRequest{TID: proto.TransactionID{Epoch: 1, Counter: 2}})
	if err1 != nil && err1 == ErrResourceExhausted {
		t.Fatalf("first request should pass")
	}
	if err2 == nil {
		t.Fatalf("second request should be limited")
	}
}

func TestDebugEndpointDeniedWithoutAdmin(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	a := &auth.Authorizer{Tokens: map[string]auth.Identity{"adm": {Name: "adm", Role: auth.RoleAdmin}}}
	n.ConfigureSafety(SafetyConfig{EnableDebug: true, Authorizer: a})
	s := httptest.NewServer(n.HTTPHandler(false))
	defer s.Close()
	res, err := http.Get(s.URL + "/debug/locks")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 got %d", res.StatusCode)
	}
}

func TestHTTPServerTLS(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	s := httptest.NewUnstartedServer(n.HTTPHandler(false))
	s.StartTLS()
	defer s.Close()
	c := s.Client()
	resp, err := c.Get(s.URL + "/health")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("tls client failed: %v code=%v", err, resp.StatusCode)
	}
	if len(s.TLS.Certificates) == 0 {
		t.Fatalf("expected tls cert")
	}
	_ = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.TLS.Certificates[0].Certificate[0]})
	_ = &tls.Config{}
}

type denyPolicy struct{}

func (denyPolicy) EvaluatePolicy(actorID, tenantID, projectID, role, action, resource string, revocationEpoch uint64) (bool, string) {
	return false, action + " denied"
}

type allowPolicy struct{ action string }

func (p *allowPolicy) EvaluatePolicy(actorID, tenantID, projectID, role, action, resource string, revocationEpoch uint64) (bool, string) {
	p.action = action
	return true, "allow"
}

func TestMemnodeControlPlaneUsesPolicyBeforeCoarseRole(t *testing.T) {
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{Authorizer: &auth.Authorizer{Policy: denyPolicy{}}})
	ctx := auth.WithIdentity(context.Background(), auth.Identity{Name: "mgr", Role: auth.RoleManager})
	_, err := n.ConfigureReplication(ctx, proto.ConfigureReplicationRequest{Role: proto.ReplicationRoleSingle})
	if err == nil {
		t.Fatalf("expected policy denial despite manager role")
	}
}

func TestMemnodeControlPlaneReportsPolicyAction(t *testing.T) {
	p := &allowPolicy{}
	n := NewParticipantWithId(1, 256)
	n.ConfigureSafety(SafetyConfig{Authorizer: &auth.Authorizer{Policy: p}})
	ctx := auth.WithIdentity(context.Background(), auth.Identity{Name: "mgr", Role: auth.RoleClient})
	_, err := n.ConfigureReplication(ctx, proto.ConfigureReplicationRequest{Role: proto.ReplicationRoleSingle})
	if err != nil {
		t.Fatalf("expected policy allow to bypass coarse role: %v", err)
	}
	if p.action != "memnode.configure_replication" {
		t.Fatalf("unexpected action %q", p.action)
	}
}
