package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"strings"
)

type Role string

const (
	RoleClient  Role = "client"
	RoleManager Role = "manager"
	RoleAdmin   Role = "admin"
	RolePeer    Role = "peer"
)

type Identity struct {
	Name            string
	Role            Role
	TenantID        string
	ProjectID       string
	RevocationEpoch uint64
}

type contextKey string

const identityKey contextKey = "concertum_identity"

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey, id)
}

func FromContext(ctx context.Context) (Identity, bool) {
	v := ctx.Value(identityKey)
	id, ok := v.(Identity)
	return id, ok
}

type PolicyEvaluator interface {
	EvaluatePolicy(actorID, tenantID, projectID, role, action, resource string, revocationEpoch uint64) (bool, string)
}

type Authorizer struct {
	Tokens map[string]Identity
	MTLS   map[string]Identity
	Policy PolicyEvaluator
}

func (a *Authorizer) AuthenticateHTTP(r *http.Request) (Identity, error) {
	if a == nil {
		return Identity{}, errors.New("unauthenticated")
	}
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		if id, ok := a.identityFromCert(r.TLS.PeerCertificates[0]); ok {
			return id, nil
		}
	}
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(h), "bearer ") {
		tok := strings.TrimSpace(h[7:])
		if id, ok := a.Tokens[tok]; ok {
			return id, nil
		}
	}
	return Identity{}, errors.New("unauthenticated")
}

func (a *Authorizer) AuthenticateTLS(state *tls.ConnectionState) (Identity, error) {
	if a == nil {
		return Identity{}, errors.New("unauthenticated")
	}
	if state == nil || len(state.PeerCertificates) == 0 {
		return Identity{}, errors.New("unauthenticated")
	}
	if id, ok := a.identityFromCert(state.PeerCertificates[0]); ok {
		return id, nil
	}
	return Identity{}, errors.New("unauthenticated")
}

func (a *Authorizer) identityFromCert(cert *x509.Certificate) (Identity, bool) {
	if cert == nil {
		return Identity{}, false
	}
	if len(cert.DNSNames) > 0 {
		for _, dns := range cert.DNSNames {
			if id, ok := a.MTLS[dns]; ok {
				return id, true
			}
		}
	}
	cn := strings.TrimSpace(cert.Subject.CommonName)
	id, ok := a.MTLS[cn]
	return id, ok
}

func (a *Authorizer) Authorize(ctx context.Context, action, resource string, fallbackRoles ...Role) error {
	if a == nil {
		return nil
	}
	id, ok := FromContext(ctx)
	if !ok {
		return errors.New("unauthenticated")
	}
	if a.Policy != nil {
		allowed, reason := a.Policy.EvaluatePolicy(id.Name, id.TenantID, id.ProjectID, string(id.Role), action, resource, id.RevocationEpoch)
		if !allowed {
			return errors.New("forbidden: " + reason)
		}
		return nil
	}
	allowed := map[Role]struct{}{}
	for _, r := range fallbackRoles {
		allowed[r] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	if _, ok := allowed[id.Role]; !ok {
		return errors.New("forbidden")
	}
	return nil
}

func RequireRole(next http.HandlerFunc, a *Authorizer, roles ...Role) http.HandlerFunc {
	allowed := map[Role]struct{}{}
	for _, r := range roles {
		allowed[r] = struct{}{}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if a == nil {
			next(w, r)
			return
		}
		id, err := a.AuthenticateHTTP(r)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		if a.Policy != nil {
			allowedByPolicy, _ := a.Policy.EvaluatePolicy(id.Name, id.TenantID, id.ProjectID, string(id.Role), "http."+strings.ToLower(r.Method), r.URL.Path, id.RevocationEpoch)
			if !allowedByPolicy {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"forbidden"}`))
				return
			}
		} else if _, ok := allowed[id.Role]; !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"forbidden"}`))
			return
		}
		next(w, r.WithContext(WithIdentity(r.Context(), id)))
	}
}
