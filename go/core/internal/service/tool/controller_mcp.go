package tool

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// controllerMCPPath is where the controller mounts its own MCP server.
const controllerMCPPath = "/mcp"

// ControllerMCPEndpoint is the controller's own /mcp endpoint, served
// in-process instead of over the network.
//
// The chart's kagent-api RemoteMCPServer points at the controller itself.
// Calling it over HTTP makes the controller authenticate to itself, which has
// no credential to present under trusted-proxy (or any mode that needs one).
// Serving it in-process skips the network and the authenticator: listing tools
// needs no identity, and a tool call carries the caller's own auth session in
// its context, so every service still authorizes it as that caller and rejects
// it when there is none.
type ControllerMCPEndpoint struct {
	scheme    string
	authority string
	handler   http.Handler
}

// NewControllerMCPEndpoint describes the controller's /mcp endpoint at
// controlPlaneURL (the URL runtimes use to reach the controller), served by
// handler. handler must be the MCP handler without the authentication
// middleware.
func NewControllerMCPEndpoint(controlPlaneURL string, handler http.Handler) (*ControllerMCPEndpoint, error) {
	if handler == nil {
		return nil, fmt.Errorf("controller MCP handler is required")
	}
	scheme, authority, err := canonicalOrigin(controlPlaneURL)
	if err != nil {
		return nil, fmt.Errorf("controller control-plane URL: %w", err)
	}
	return &ControllerMCPEndpoint{scheme: scheme, authority: authority, handler: handler}, nil
}

// Serves reports whether rawURL addresses this endpoint: the same scheme,
// host and port as the control-plane URL, and the /mcp path.
func (e *ControllerMCPEndpoint) Serves(rawURL string) bool {
	if e == nil {
		return false
	}
	scheme, authority, err := canonicalOrigin(rawURL)
	if err != nil {
		return false
	}
	parsed, _ := url.Parse(rawURL) // canonicalOrigin parsed it already.
	return scheme == e.scheme && authority == e.authority && parsed.Path == controllerMCPPath
}

func (e *ControllerMCPEndpoint) httpClient() *http.Client {
	return &http.Client{Transport: inProcessTransport{handler: e.handler}}
}

// canonicalOrigin returns the lower-case scheme and host:port of an absolute
// http or https URL, filling in the scheme's default port.
func canonicalOrigin(rawURL string) (string, string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", "", err
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", fmt.Errorf("URL %q must be absolute http or https", rawURL)
	}
	if parsed.User != nil {
		return "", "", fmt.Errorf("URL %q must not carry user info", rawURL)
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", "", fmt.Errorf("URL %q has no host", rawURL)
	}
	port := parsed.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[scheme]
	}
	return scheme, net.JoinHostPort(host, port), nil
}

// inProcessTransport answers HTTP requests by calling a handler directly. The
// controller's MCP handler is stateless, so each request completes when the
// handler returns and the response can be buffered.
type inProcessTransport struct {
	handler http.Handler
}

func (t inProcessTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	recorder := &bufferedResponse{header: http.Header{}}
	t.handler.ServeHTTP(recorder, request)
	if request.Body != nil {
		_ = request.Body.Close()
	}
	status := recorder.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        recorder.header,
		Body:          io.NopCloser(bytes.NewReader(recorder.body.Bytes())),
		ContentLength: int64(recorder.body.Len()),
		Request:       request,
	}, nil
}

// bufferedResponse is the minimal http.ResponseWriter the MCP handler needs.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *bufferedResponse) Header() http.Header { return r.header }

func (r *bufferedResponse) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}

func (r *bufferedResponse) Write(data []byte) (int, error) {
	r.WriteHeader(http.StatusOK)
	return r.body.Write(data)
}

// Flush is a no-op: the response is delivered whole when the handler returns.
func (r *bufferedResponse) Flush() {}
