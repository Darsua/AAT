package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var fixedTestTime = time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)

func TestIssueToken(t *testing.T) {
	service, handler, signingSecret, clientSecret := newTestAuth(t)
	service.now = func() time.Time { return fixedTestTime }

	requestBody := tokenRequest{
		ClientID:       "media-test-client",
		ClientSecret:   clientSecret,
		RequestedScope: "media",
	}
	response := performJSONRequest(t, handler, http.MethodPost, "/auth/token", requestBody)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", response.Code, response.Body.String())
	}

	var payload tokenResponse
	decodeTestResponse(t, response, &payload)
	if payload.TokenType != "Bearer" || payload.ExpiresIn != 60 {
		t.Fatalf("unexpected token metadata: %+v", payload)
	}
	if payload.Scope != "hazards:read:summary" || payload.RefreshToken == "" {
		t.Fatalf("unexpected scope or refresh token: %+v", payload)
	}

	claims := &accessClaims{}
	token, err := jwt.ParseWithClaims(payload.AccessToken, claims, func(token *jwt.Token) (any, error) {
		return []byte(signingSecret), nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(issuer),
		jwt.WithTimeFunc(func() time.Time { return fixedTestTime }),
	)
	if err != nil || !token.Valid {
		t.Fatalf("issued access token is invalid: %v", err)
	}
	if claims.Subject != "media-test-client" || claims.Role != "media" || claims.Scope != "hazards:read:summary" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	if claims.IssuedAt == nil || claims.ExpiresAt == nil || claims.ExpiresAt.Sub(claims.IssuedAt.Time) != time.Minute {
		t.Fatalf("expected a 60-second token lifetime, got iat=%v exp=%v", claims.IssuedAt, claims.ExpiresAt)
	}
}

func TestIssueTokenRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name       string
		body       func(clientSecret string) any
		wantStatus int
	}{
		{
			name: "invalid client secret",
			body: func(_ string) any {
				return tokenRequest{ClientID: "media-test-client", ClientSecret: "invalid", RequestedScope: "media"}
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "role escalation",
			body: func(clientSecret string) any {
				return tokenRequest{ClientID: "media-test-client", ClientSecret: clientSecret, RequestedScope: "internal"}
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "missing required field",
			body: func(clientSecret string) any {
				return tokenRequest{ClientID: "media-test-client", ClientSecret: clientSecret}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, handler, _, clientSecret := newTestAuth(t)
			response := performJSONRequest(t, handler, http.MethodPost, "/auth/token", test.body(clientSecret))
			if response.Code != test.wantStatus {
				t.Fatalf("expected status %d, got %d: %s", test.wantStatus, response.Code, response.Body.String())
			}
		})
	}

	_, handler, _, _ := newTestAuth(t)
	request := httptest.NewRequest(http.MethodPost, "/auth/token", strings.NewReader("{"))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed JSON to return 400, got %d", response.Code)
	}
}

func TestRefreshRotationAndReplayRevocation(t *testing.T) {
	service, handler, _, clientSecret := newTestAuth(t)
	service.now = func() time.Time { return fixedTestTime }

	issued := issueTestToken(t, handler, clientSecret)
	rotatedResponse := performJSONRequest(t, handler, http.MethodPost, "/auth/refresh", refreshRequest{RefreshToken: issued.RefreshToken})
	if rotatedResponse.Code != http.StatusOK {
		t.Fatalf("expected refresh status 200, got %d: %s", rotatedResponse.Code, rotatedResponse.Body.String())
	}
	var rotated tokenResponse
	decodeTestResponse(t, rotatedResponse, &rotated)
	if rotated.RefreshToken == "" || rotated.RefreshToken == issued.RefreshToken {
		t.Fatal("refresh token was not rotated")
	}

	replayResponse := performJSONRequest(t, handler, http.MethodPost, "/auth/refresh", refreshRequest{RefreshToken: issued.RefreshToken})
	if replayResponse.Code != http.StatusUnauthorized {
		t.Fatalf("expected replay to return 401, got %d", replayResponse.Code)
	}

	rotatedAfterReplay := performJSONRequest(t, handler, http.MethodPost, "/auth/refresh", refreshRequest{RefreshToken: rotated.RefreshToken})
	if rotatedAfterReplay.Code != http.StatusUnauthorized {
		t.Fatalf("expected the rotated token's session to be revoked, got %d", rotatedAfterReplay.Code)
	}
}

func TestExpiredRefreshTokenIsRejected(t *testing.T) {
	service, handler, _, clientSecret := newTestAuth(t)
	currentTime := fixedTestTime
	service.now = func() time.Time { return currentTime }
	issued := issueTestToken(t, handler, clientSecret)

	currentTime = currentTime.Add(refreshTokenTTL)
	response := performJSONRequest(t, handler, http.MethodPost, "/auth/refresh", refreshRequest{RefreshToken: issued.RefreshToken})
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected expired refresh token to return 401, got %d", response.Code)
	}
}

func TestClientRegistryRejectsDuplicateIDs(t *testing.T) {
	mediaSecret := testRandomValue(t)
	fieldSecret := testRandomValue(t)
	internalSecret := testRandomValue(t)
	t.Setenv("AUTH_MEDIA_CLIENT_ID", "duplicate-client")
	t.Setenv("AUTH_FIELD_CLIENT_ID", "duplicate-client")
	t.Setenv("AUTH_INTERNAL_CLIENT_ID", "internal-test-client")
	t.Setenv("AUTH_MEDIA_CLIENT_SECRET", mediaSecret)
	t.Setenv("AUTH_FIELD_CLIENT_SECRET", fieldSecret)
	t.Setenv("AUTH_INTERNAL_CLIENT_SECRET", internalSecret)

	if _, err := clientRegistryFromEnvironment(); err == nil || !strings.Contains(err.Error(), "duplicate client ID") {
		t.Fatalf("expected duplicate client ID error, got %v", err)
	}
}

func newTestAuth(t *testing.T) (*authService, http.Handler, string, string) {
	t.Helper()
	signingSecret := testRandomValue(t)
	clientSecret := testRandomValue(t)
	service := newAuthService(signingSecret, time.Minute, map[string]clientCredential{
		"media-test-client": {
			Secret: clientSecret,
			Role:   "media",
			Scope:  "hazards:read:summary",
		},
	})
	return service, newAuthHandler(service).routes(), signingSecret, clientSecret
}

func issueTestToken(t *testing.T, handler http.Handler, clientSecret string) tokenResponse {
	t.Helper()
	response := performJSONRequest(t, handler, http.MethodPost, "/auth/token", tokenRequest{
		ClientID:       "media-test-client",
		ClientSecret:   clientSecret,
		RequestedScope: "media",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("issue token failed with status %d: %s", response.Code, response.Body.String())
	}
	var payload tokenResponse
	decodeTestResponse(t, response, &payload)
	return payload
}

func performJSONRequest(t *testing.T, handler http.Handler, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(method, target, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeTestResponse(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func testRandomValue(t *testing.T) string {
	t.Helper()
	value, err := randomValue(32)
	if err != nil {
		t.Fatalf("generate test secret: %v", err)
	}
	return value
}
