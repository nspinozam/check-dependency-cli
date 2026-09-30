package checker

import "testing"

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
