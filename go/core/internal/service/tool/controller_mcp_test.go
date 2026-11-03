package tool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

func TestControllerMCPEndpointServes(t *testing.T) {
	endpoint, err := NewControllerMCPEndpoint("http://kagent-controller.kagent.svc.cluster.local:8083", http.NotFoundHandler())
	require.NoError(t, err)

	scenarios := []struct {
		name string
		url  string
		want bool
	}{
		{"the control-plane origin with the mcp path", "http://kagent-controller.kagent.svc.cluster.local:8083/mcp", true},
		{"an upper-case host", "http://KAGENT-CONTROLLER.kagent.svc.cluster.local:8083/mcp", true},
		{"another host", "http://kagent-controller.other.svc.cluster.local:8083/mcp", false},
		{"the short service name", "http://kagent-controller.kagent:8083/mcp", false},
		{"another port", "http://kagent-controller.kagent.svc.cluster.local:8080/mcp", false},
		{"another scheme", "https://kagent-controller.kagent.svc.cluster.local:8083/mcp", false},
		{"another path", "http://kagent-controller.kagent.svc.cluster.local:8083/api/mcp", false},
		{"a sub-path of mcp", "http://kagent-controller.kagent.svc.cluster.local:8083/mcp/extra", false},
		{"user info", "http://user@kagent-controller.kagent.svc.cluster.local:8083/mcp", false},
		{"an unparsable URL", "http://[::1", false},
	}
	for _, scenario := range scenarios {
		t.Run("Given the controller's endpoint, when a RemoteMCPServer URL is "+scenario.name+", assert that it is served in-process only for the controller's own /mcp", func(t *testing.T) {
			assert.Equal(t, scenario.want, endpoint.Serves(scenario.url))
		})
	}

	t.Run("Given a control-plane URL without a port, when the URL names the default port, assert that it is served in-process", func(t *testing.T) {
		implicit, err := NewControllerMCPEndpoint("http://kagent-controller.kagent", http.NotFoundHandler())
		require.NoError(t, err)
		assert.True(t, implicit.Serves("http://kagent-controller.kagent:80/mcp"))
	})

	t.Run("Given no controller endpoint, when any URL is checked, assert that nothing is served in-process", func(t *testing.T) {
		var none *ControllerMCPEndpoint
		assert.False(t, none.Serves("http://kagent-controller.kagent.svc.cluster.local:8083/mcp"))
	})

	t.Run("Given a control-plane URL that is not absolute http, when the endpoint is built, assert that it is rejected", func(t *testing.T) {
		_, err := NewControllerMCPEndpoint("kagent-controller:8083", http.NotFoundHandler())
		require.Error(t, err)
	})
}

func TestRuntimeMCPClientControllerMCP(t *testing.T) {
	t.Run("Given a RemoteMCPServer at the controller's own /mcp behind an authenticator, when the controller discovers its tools, assert that they are listed in-process without a network call", func(t *testing.T) {
		f := newControllerMCPFixture(t)
		client := NewRuntimeMCPClient(toolTestKube(t, f.server(f.network.URL+"/mcp")), WithControllerMCP(f.endpoint(t)))

		tools, err := client.ListTools(t.Context(), f.ref)

		require.NoError(t, err)
		require.Len(t, tools, 1)
		assert.Equal(t, "whoami", tools[0].Name)
		assert.Zero(t, f.networkRequests.Load(), "the controller's own /mcp must not be called over the network")
	})

	t.Run("Given a RemoteMCPServer at the controller's own /mcp behind an authenticator, when discovery has no in-process endpoint, assert that the network call is rejected as unauthorized", func(t *testing.T) {
		f := newControllerMCPFixture(t)
		client := NewRuntimeMCPClient(toolTestKube(t, f.server(f.network.URL+"/mcp")))

		_, err := client.ListTools(t.Context(), f.ref)

		require.Error(t, err)
		assert.ErrorContains(t, err, "Unauthorized")
		assert.NotZero(t, f.networkRequests.Load())
	})

	t.Run("Given a RemoteMCPServer at another origin, when the controller discovers its tools, assert that the call still goes over the network", func(t *testing.T) {
		f := newControllerMCPFixture(t)
		other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}))
		t.Cleanup(other.Close)
		client := NewRuntimeMCPClient(toolTestKube(t, f.server(other.URL+"/mcp")), WithControllerMCP(f.endpoint(t)))

		_, err := client.ListTools(t.Context(), f.ref)

		require.Error(t, err)
		assert.ErrorContains(t, err, "Unauthorized")
	})

	t.Run("Given an authenticated caller, when it calls a controller tool in-process, assert that the tool runs as that caller", func(t *testing.T) {
		f := newControllerMCPFixture(t)
		client := NewRuntimeMCPClient(toolTestKube(t, f.server(f.network.URL+"/mcp")), WithControllerMCP(f.endpoint(t)))
		ctx := auth.AuthSessionTo(t.Context(), testCaller{id: "alice@example.com"})

		result, err := client.CallTool(ctx, f.ref, "whoami", map[string]any{})

		require.NoError(t, err)
		assert.Equal(t, "alice@example.com", resultText(t, result))
	})

	t.Run("Given no authenticated caller, when a controller tool is called in-process, assert that the tool sees no identity", func(t *testing.T) {
		f := newControllerMCPFixture(t)
		client := NewRuntimeMCPClient(toolTestKube(t, f.server(f.network.URL+"/mcp")), WithControllerMCP(f.endpoint(t)))

		result, err := client.CallTool(t.Context(), f.ref, "whoami", map[string]any{})

		require.NoError(t, err)
		assert.Equal(t, "anonymous", resultText(t, result))
	})
}

// controllerMCPFixture stands in for the controller: an MCP handler with one
// tool that reports the caller's identity, and a network listener for the same
// handler that rejects every request, as the controller's authenticator does
// for a caller with no credential.
type controllerMCPFixture struct {
	handler         http.Handler
	network         *httptest.Server
	networkRequests atomic.Int32
	ref             MCPServerRef
}

func newControllerMCPFixture(t *testing.T) *controllerMCPFixture {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "kagent", Version: "test"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "whoami", Description: "Report the caller"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			caller := "anonymous"
			if session, ok := auth.AuthSessionFrom(ctx); ok {
				caller = session.Principal().ID
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: caller}}}, nil, nil
		})
	f := &controllerMCPFixture{
		handler: mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true}),
		ref:     MCPServerRef{Ref: types.NamespacedName{Namespace: "kagent", Name: "kagent-api"}, GroupKind: "RemoteMCPServer.api.kagent.dev"},
	}
	f.network = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.networkRequests.Add(1)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}))
	t.Cleanup(f.network.Close)
	return f
}

func (f *controllerMCPFixture) endpoint(t *testing.T) *ControllerMCPEndpoint {
	t.Helper()
	endpoint, err := NewControllerMCPEndpoint(f.network.URL, f.handler)
	require.NoError(t, err)
	return endpoint
}

func (f *controllerMCPFixture) server(url string) *v1alpha3.RemoteMCPServer {
	return &v1alpha3.RemoteMCPServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.ref.Ref.Namespace, Name: f.ref.Ref.Name},
		Spec:       v1alpha3.RemoteMCPServerSpec{URL: url, Protocol: v1alpha3.RemoteMCPServerProtocolStreamableHttp},
	}
}

type testCaller struct{ id string }

func (c testCaller) Principal() auth.Principal { return auth.Principal{ID: c.id} }

func resultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.NotNil(t, result)
	require.Len(t, result.Content, 1)
	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok, "expected text content, got %T", result.Content[0])
	return text.Text
}
