package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDaprSecretReference(t *testing.T) {
	tests := []struct {
		name      string
		entry     map[string]interface{}
		wantName  string
		wantKey   string
		wantFound bool
		wantError bool
	}{
		{
			name: "valid reference",
			entry: map[string]interface{}{
				"secretKeyRef": map[string]interface{}{
					"name": "database-credentials",
					"key":  "password",
				},
			},
			wantName:  "database-credentials",
			wantKey:   "password",
			wantFound: true,
		},
		{
			name: "metadata without reference",
			entry: map[string]interface{}{
				"name":  "password",
				"value": "unused",
			},
		},
		{
			name: "reference missing key",
			entry: map[string]interface{}{
				"secretKeyRef": map[string]interface{}{
					"name": "database-credentials",
				},
			},
			wantFound: true,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, key, found, err := daprSecretReference(test.entry)
			if (err != nil) != test.wantError {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != test.wantName || key != test.wantKey || found != test.wantFound {
				t.Fatalf("got name=%q key=%q found=%v, want name=%q key=%q found=%v", name, key, found, test.wantName, test.wantKey, test.wantFound)
			}
		})
	}
}

func TestValidateDaprSecretUsesSidecar(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1.0/secrets/secret/db" {
			t.Fatalf("got path %q, want %q", request.URL.Path, "/v1.0/secrets/secret/db")
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"MONGO_DB":"mongodb://mongo"}`))
	}))
	defer server.Close()

	err := validateDaprSecret(context.Background(), server.URL, Dependency{
		Name:        "db",
		Key:         "MONGO_DB",
		SecretStore: "secret",
	})
	if err != nil {
		t.Fatalf("validate Dapr secret: %v", err)
	}
}

func TestValidateDaprSecretMissingKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte(`{"OTHER":"value"}`))
	}))
	defer server.Close()

	err := validateDaprSecret(context.Background(), server.URL, Dependency{
		Name:        "db",
		Key:         "MONGO_DB",
		SecretStore: "secret",
	})
	if err == nil {
		t.Fatal("expected missing key error")
	}
}
