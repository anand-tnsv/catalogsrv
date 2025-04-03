package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func executeTestRequest(t *testing.T, req *http.Request, apiKey *string) *httptest.ResponseRecorder {
	s, err := CreateNewServer()
	assert.NoError(t, err, "create new server")

	if apiKey != nil {
		_ = apiKey
		//auth.SignApiRequest(req, apiKey.KeyId, apiKey.PrivKey)
	}

	// Mount Handlers
	s.MountHandlers()

	rr := httptest.NewRecorder()
	s.Router.ServeHTTP(rr, req)

	return rr
}

func checkHeader(t *testing.T, h http.Header) {
	expected := "application/json"
	got := h.Get("Content-Type")
	assert.Equal(t, expected, got, "Content-Type expected %s, got %s", expected, got)
	assert.NotNil(t, h.Get("X-Request-ID"), "No Request Id")
}

func compareJson(t *testing.T, expected any, actual string) {
	j, err := json.Marshal(expected)
	assert.NoError(t, err, "json marshal")
	assert.JSONEq(t, string(j), actual, "Expected: %v\n Got: %v\n", expected, actual)
}

func setRequestBodyAndHeader(t *testing.T, req *http.Request, data interface{}) {
	// Marshal the data into JSON
	jsonData, err := json.Marshal(data)
	assert.NoError(t, err, "Failed to marshal data into JSON")

	// Set the request body to the JSON
	req.Body = io.NopCloser(bytes.NewReader(jsonData))
	req.ContentLength = int64(len(jsonData))

	// Set the Content-Type header to application/json
	req.Header.Set("Content-Type", "application/json")
}
