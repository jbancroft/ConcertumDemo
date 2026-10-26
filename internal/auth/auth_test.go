package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticateHTTPNilAuthorizer(t *testing.T) {
	var authorizer *Authorizer
	if _, err := authorizer.AuthenticateHTTP(httptest.NewRequest("GET", "/", nil)); err == nil {
		t.Fatal("expected nil authorizer to reject authentication")
	}
}

func FuzzAuthenticateHTTPNeverPanics(f *testing.F) {
	f.Add("")
	f.Add("Bearer token")
	f.Fuzz(func(t *testing.T, header string) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", header)
		_, _ = (&Authorizer{}).AuthenticateHTTP(req)
	})
}
