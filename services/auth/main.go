package main

// This service stays in one file to keep the milestone implementation self-contained.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	issuer          = "bnpb-auth-service"
	refreshTokenTTL = 24 * time.Hour
)

var (
	errInvalidCredentials = errors.New("invalid credentials")
	errForbiddenScope     = errors.New("forbidden scope")
	errInvalidGrant       = errors.New("invalid refresh token grant")
)

// Data models.
type config struct {
	Port      string
	JWTSecret string
	AccessTTL time.Duration
	Clients   map[string]clientCredential
}

type clientCredential struct {
	Secret string
	Role   string
	Scope  string
}

type tokenRequest struct {
	ClientID       string `json:"client_id"`
	ClientSecret   string `json:"client_secret"`
	RequestedScope string `json:"requested_scope"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	TokenType    string `json:"token_type"`
	AccessToken  string `json:"access_token"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type accessClaims struct {
	Role  string `json:"role"`
	Scope string `json:"scope"`
	jwt.RegisteredClaims
}

type refreshRecord struct {
	ClientID  string
	Role      string
	Scope     string
	SessionID string
	ExpiresAt time.Time
	Used      bool
	Revoked   bool
}

// In-memory refresh-token store.
type refreshStore struct {
	mu              sync.Mutex
	records         map[string]*refreshRecord
	revokedSessions map[string]time.Time
}

func newRefreshStore() *refreshStore {
	return &refreshStore{
		records:         make(map[string]*refreshRecord),
		revokedSessions: make(map[string]time.Time),
	}
}

// create stores a token hash and removes expired records lazily.
func (s *refreshStore) create(token string, record refreshRecord, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(now)
	if !now.Before(record.ExpiresAt) {
		return errInvalidGrant
	}
	if _, revoked := s.revokedSessions[record.SessionID]; revoked {
		return errInvalidGrant
	}
	s.records[tokenHash(token)] = &record
	return nil
}

func (s *refreshStore) consume(token string, now time.Time) (refreshRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(now)

	record, exists := s.records[tokenHash(token)]
	if !exists || record.Revoked {
		return refreshRecord{}, errInvalidGrant
	}
	if record.Used {
		s.revokeSessionLocked(record.SessionID)
		return refreshRecord{}, errInvalidGrant
	}
	record.Used = true
	return *record, nil
}

func (s *refreshStore) revokeSessionLocked(sessionID string) {
	var expiresAt time.Time
	for _, record := range s.records {
		if record.SessionID == sessionID {
			record.Revoked = true
			if record.ExpiresAt.After(expiresAt) {
				expiresAt = record.ExpiresAt
			}
		}
	}
	if !expiresAt.IsZero() {
		s.revokedSessions[sessionID] = expiresAt
	}
}

func (s *refreshStore) cleanupExpiredLocked(now time.Time) {
	for hash, record := range s.records {
		if !now.Before(record.ExpiresAt) {
			delete(s.records, hash)
		}
	}
	for sessionID, expiresAt := range s.revokedSessions {
		if !now.Before(expiresAt) {
			delete(s.revokedSessions, sessionID)
		}
	}
}

// Authentication and token issuance.
type authService struct {
	jwtSecret []byte
	accessTTL time.Duration
	clients   map[string]clientCredential
	store     *refreshStore
	now       func() time.Time
}

// newAuthService creates the authentication service and token store.
func newAuthService(secret string, accessTTL time.Duration, clients map[string]clientCredential) *authService {
	return &authService{
		jwtSecret: []byte(secret),
		accessTTL: accessTTL,
		clients:   clients,
		store:     newRefreshStore(),
		now:       time.Now,
	}
}

// authenticate validates credentials and issues an access-refresh token pair.
func (s *authService) authenticate(clientID, clientSecret, requestedScope string) (tokenResponse, error) {
	client, exists := s.clients[clientID]
	if !exists || !secretsEqual(client.Secret, clientSecret) {
		return tokenResponse{}, errInvalidCredentials
	}
	if requestedScope != client.Role && requestedScope != client.Scope {
		return tokenResponse{}, errForbiddenScope
	}
	return s.issueTokenPair(clientID, client.Role, client.Scope, "")
}

func (s *authService) refresh(refreshToken string) (tokenResponse, error) {
	record, err := s.store.consume(refreshToken, s.now().UTC())
	if err != nil {
		return tokenResponse{}, err
	}
	return s.issueTokenPair(record.ClientID, record.Role, record.Scope, record.SessionID)
}

func (s *authService) issueTokenPair(clientID, role, scope, sessionID string) (tokenResponse, error) {
	now := s.now().UTC()
	claims := accessClaims{
		Role:  role,
		Scope: scope,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   clientID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
	}
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
	if err != nil {
		return tokenResponse{}, err
	}

	if sessionID == "" {
		sessionID, err = randomValue(16)
		if err != nil {
			return tokenResponse{}, err
		}
	}
	refreshToken, err := randomValue(32)
	if err != nil {
		return tokenResponse{}, err
	}
	if err := s.store.create(refreshToken, refreshRecord{
		ClientID:  clientID,
		Role:      role,
		Scope:     scope,
		SessionID: sessionID,
		ExpiresAt: now.Add(refreshTokenTTL),
	}, now); err != nil {
		return tokenResponse{}, err
	}

	return tokenResponse{
		TokenType:    "Bearer",
		AccessToken:  accessToken,
		ExpiresIn:    int64(s.accessTTL / time.Second),
		RefreshToken: refreshToken,
		Scope:        scope,
	}, nil
}

// HTTP handlers.
type authHandler struct {
	service *authService
}

func newAuthHandler(service *authService) *authHandler {
	return &authHandler{service: service}
}

func (h *authHandler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /auth/token", h.issueToken)
	mux.HandleFunc("POST /auth/refresh", h.refreshToken)
	return mux
}

func (h *authHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "UP", Service: "auth"})
}

func (h *authHandler) issueToken(w http.ResponseWriter, r *http.Request) {
	var request tokenRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: "Invalid JSON request body"})
		return
	}
	if request.ClientID == "" || request.ClientSecret == "" || request.RequestedScope == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: "client_id, client_secret, and requested_scope are required"})
		return
	}

	response, err := h.service.authenticate(request.ClientID, request.ClientSecret, request.RequestedScope)
	switch {
	case errors.Is(err, errInvalidCredentials):
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "UNAUTHORIZED", Message: "Invalid client_id or client_secret"})
	case errors.Is(err, errForbiddenScope):
		writeJSON(w, http.StatusForbidden, errorResponse{Error: "FORBIDDEN", Message: "Client is not authorized to request this scope"})
	case err != nil:
		log.Printf("[auth] issue token: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "INTERNAL_ERROR", Message: "Unable to issue token"})
	default:
		writeJSON(w, http.StatusOK, response)
	}
}

func (h *authHandler) refreshToken(w http.ResponseWriter, r *http.Request) {
	var request refreshRequest
	if err := decodeJSON(w, r, &request); err != nil || request.RefreshToken == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "BAD_REQUEST", Message: "refresh_token is required"})
		return
	}

	response, err := h.service.refresh(request.RefreshToken)
	switch {
	case errors.Is(err, errInvalidGrant):
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid_grant", Message: "Refresh token expired, invalid, or already rotated"})
	case err != nil:
		log.Printf("[auth] rotate refresh token: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "INTERNAL_ERROR", Message: "Unable to issue token"})
	default:
		writeJSON(w, http.StatusOK, response)
	}
}

// Configuration and helpers.
func loadConfig() (config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}

	jwtSecret, err := requiredEnvironment("JWT_SECRET")
	if err != nil {
		return config{}, err
	}
	accessTTL, err := accessTTLFromEnvironment()
	if err != nil {
		return config{}, err
	}
	clients, err := clientRegistryFromEnvironment()
	if err != nil {
		return config{}, err
	}

	return config{Port: port, JWTSecret: jwtSecret, AccessTTL: accessTTL, Clients: clients}, nil
}

func accessTTLFromEnvironment() (time.Duration, error) {
	seconds := int64(60)
	if raw := os.Getenv("JWT_TTL_SECONDS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("JWT_TTL_SECONDS must be a positive integer")
		}
		seconds = parsed
	}
	return time.Duration(seconds) * time.Second, nil
}

func clientRegistryFromEnvironment() (map[string]clientCredential, error) {
	clients := make(map[string]clientCredential)
	definitions := []struct {
		idEnvironment     string
		defaultID         string
		secretEnvironment string
		role              string
		scope             string
	}{
		{"AUTH_MEDIA_CLIENT_ID", "test-media-client", "AUTH_MEDIA_CLIENT_SECRET", "media", "hazards:read:summary"},
		{"AUTH_FIELD_CLIENT_ID", "test-field-client", "AUTH_FIELD_CLIENT_SECRET", "field", "hazards:read:all"},
		{"AUTH_INTERNAL_CLIENT_ID", "test-internal-client", "AUTH_INTERNAL_CLIENT_SECRET", "internal", "hazards:read:all"},
	} // Roles and scopes are fixed; credentials are loaded from the environment.

	for _, definition := range definitions {
		clientID := strings.TrimSpace(os.Getenv(definition.idEnvironment))
		if clientID == "" {
			clientID = definition.defaultID
		}
		secret, err := requiredEnvironment(definition.secretEnvironment)
		if err != nil {
			return nil, err
		}
		if _, exists := clients[clientID]; exists {
			return nil, fmt.Errorf("duplicate client ID configured: %q", clientID)
		}
		clients[clientID] = clientCredential{Secret: secret, Role: definition.role, Scope: definition.scope}
	}
	return clients, nil
}

func requiredEnvironment(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("required environment variable %s is not set", name)
	}
	return value, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func secretsEqual(expected, supplied string) bool {
	return subtle.ConstantTimeCompare([]byte(expected), []byte(supplied)) == 1
}

func randomValue(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func tokenHash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[auth] invalid configuration: %v", err)
	}

	service := newAuthService(cfg.JWTSecret, cfg.AccessTTL, cfg.Clients)
	handler := newAuthHandler(service)
	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler.routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("[auth] server listening on port %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[auth] server error: %v", err)
		}
	}()

	<-stop
	log.Println("[auth] shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[auth] forced shutdown: %v", err)
	}
}
