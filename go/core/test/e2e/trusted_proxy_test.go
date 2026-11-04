package e2e_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
)

// Under trusted-proxy the controller authenticates its own /mcp, so a
// RemoteMCPServer that points at the controller (the chart's kagent-api) must
// still discover its tools and serve calls without a credential of its own.
// Keep the parent sequential: it rolls the controller with a changed config.
// The API endpoint must survive pod replacement (NodePort or ingress).
func TestTrustedProxyControllerMCP(t *testing.T) {
	target := interactionTarget(t)
	kube := interactionKubeClient(t)
	chartServer := &v1alpha3.RemoteMCPServer{}
	require.NoError(t, kube.Get(t.Context(), ctrlclient.ObjectKey{Namespace: "kagent", Name: "kagent-api"}, chartServer),
		"Helm must install the kagent-api RemoteMCPServer")
	updateControllerConfig(t, target, func(cfg *configv1.ControllerConfig) {
		if cfg.Server == nil {
			cfg.Server = &configv1.ControllerServerConfig{}
		}
		cfg.Server.Auth = &configv1.AuthenticationConfig{Mode: new("trusted-proxy"), UserIdClaim: "email"}
	})

	// A server created after the switch can only carry status from discovery
	// under trusted-proxy, never from an earlier install.
	server := &v1alpha3.RemoteMCPServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kagent", Name: "e2e-controller-mcp-" + uuid.NewString()[:8]},
		Spec: v1alpha3.RemoteMCPServerSpec{
			URL:      chartServer.Spec.URL,
			Protocol: v1alpha3.RemoteMCPServerProtocolStreamableHttp,
		},
	}
	require.NoError(t, kube.Create(t.Context(), server))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, ctrlclient.IgnoreNotFound(kube.Delete(ctx, server)))
	})
	tools := newTrustedProxyTools(t, target)
	reference := &apiv1.MCPServerReference{
		Ref:       &apiv1.ResourceReference{Namespace: server.Namespace, Name: server.Name},
		GroupKind: "RemoteMCPServer.api.kagent.dev",
	}

	t.Run("Given trusted-proxy authentication, when the controller discovers a RemoteMCPServer at its own /mcp, assert that it is accepted with the controller's tools", func(t *testing.T) {
		// EventuallyWithT reports the last attempt's failures, so a timeout
		// shows the discovery error rather than only that it timed out.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			observed := &v1alpha3.RemoteMCPServer{}
			if !assert.NoError(c, kube.Get(t.Context(), ctrlclient.ObjectKeyFromObject(server), observed)) {
				return
			}
			accepted := apimeta.FindStatusCondition(observed.Status.Conditions, "Accepted")
			if !assert.NotNil(c, accepted, "RemoteMCPServer has no Accepted condition yet") {
				return
			}
			assert.Equal(c, metav1.ConditionTrue, accepted.Status, "Accepted=%s %s: %s", accepted.Status, accepted.Reason, accepted.Message)
			assert.True(c, hasTool(observed.Status.DiscoveredTools, "list_agents"), "list_agents not among %d discovered tools", len(observed.Status.DiscoveredTools))
		}, 2*time.Minute, time.Second)
	})

	t.Run("Given trusted-proxy authentication, when a signed-in user calls a controller tool through the API, assert that the call succeeds", func(t *testing.T) {
		ctx := tools.asUser(t.Context(), "e2e-trusted-proxy@example.com")
		response, err := tools.client.CallMCPAppTool(ctx, &apiv1.CallMCPAppToolRequest{Server: reference, ToolName: "list_agents"})
		require.NoError(t, err)
		require.Equal(t, "MCPCallToolResult", response.GetResult().GetKind())
		require.NotNil(t, response.GetResult().GetValue().GetFields()["structuredContent"], "list_agents must return its structured result")
	})

	t.Run("Given trusted-proxy authentication, when a caller sends a tool call without a token, assert that it is rejected", func(t *testing.T) {
		_, err := tools.client.CallMCPAppTool(t.Context(), &apiv1.CallMCPAppToolRequest{Server: reference, ToolName: "list_agents"})
		require.Equal(t, codes.Unauthenticated, status.Code(err), "got %v", err)
	})
}

type trustedProxyTools struct {
	client apiv1.ToolServiceClient
}

func newTrustedProxyTools(t *testing.T, target string) *trustedProxyTools {
	t.Helper()
	return &trustedProxyTools{client: apiv1.NewToolServiceClient(newControllerConn(t, target))}
}

// asUser sends the bearer token a trusted proxy would forward. The controller
// reads the claim without verifying the signature, so a test token needs only
// the configured user ID claim.
func (*trustedProxyTools) asUser(ctx context.Context, email string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+unsignedTestJWT(map[string]any{"email": email, "sub": email}))
}

func unsignedTestJWT(claims map[string]any) string {
	segment := func(value any) string {
		encoded, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		return base64.RawURLEncoding.EncodeToString(encoded)
	}
	return segment(map[string]any{"alg": "HS256", "typ": "JWT"}) + "." + segment(claims) + "." + base64.RawURLEncoding.EncodeToString([]byte("not-verified"))
}

func hasTool(tools []*v1alpha3.MCPTool, name string) bool {
	for _, tool := range tools {
		if tool != nil && tool.Name == name {
			return true
		}
	}
	return false
}
