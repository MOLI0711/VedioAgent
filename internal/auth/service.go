package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

const SessionCookieName = "mirrorcore_session"
const SessionLifetime = 7 * 24 * time.Hour

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUnauthenticated    = errors.New("unauthenticated")
	ErrCSRF               = errors.New("invalid csrf token")
	dummyPasswordHash     []byte
)

func init() {
	// Keep failed logins for unknown users close to the bcrypt cost of known users.
	dummyPasswordHash, _ = bcrypt.GenerateFromPassword([]byte("mirrorcore-dummy-password"), bcrypt.DefaultCost)
}

type Service struct {
	pool *pgxpool.Pool
}

type User struct {
	ID    uuid.UUID
	Email string
}

type Session struct {
	User      User
	CSRFHash  []byte
	ExpiresAt time.Time
}

type LoginResult struct {
	User      User
	Token     string
	CSRFToken string
	ExpiresAt time.Time
}

func NewService(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// SeedDemoUser creates the configured demo account once. It deliberately does not
// replace the password on later restarts, so changing the env var cannot silently
// rotate credentials for an already initialized database.
func (s *Service) SeedDemoUser(ctx context.Context, email, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash bootstrap password: %w", err)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO users(id, email, password_hash)
		VALUES ($1, $2, $3) ON CONFLICT(email) DO NOTHING`, uuid.New(), strings.ToLower(strings.TrimSpace(email)), string(hash))
	if err != nil {
		return fmt.Errorf("seed demo account: %w", err)
	}
	return nil
}

func (s *Service) Login(ctx context.Context, email, password string) (LoginResult, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	var user User
	var passwordHash string
	err := s.pool.QueryRow(ctx, `SELECT id, email, password_hash FROM users WHERE email = $1`, email).
		Scan(&user.ID, &user.Email, &passwordHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return LoginResult{}, fmt.Errorf("load user: %w", err)
	}
	compareHash := []byte(passwordHash)
	if errors.Is(err, pgx.ErrNoRows) {
		compareHash = dummyPasswordHash
	}
	if len(password) > 72 || bcrypt.CompareHashAndPassword(compareHash, []byte(password)) != nil || errors.Is(err, pgx.ErrNoRows) {
		return LoginResult{}, ErrInvalidCredentials
	}

	token, tokenHash, err := newToken()
	if err != nil {
		return LoginResult{}, fmt.Errorf("create session token: %w", err)
	}
	csrfToken, csrfHash, err := newToken()
	if err != nil {
		return LoginResult{}, fmt.Errorf("create csrf token: %w", err)
	}
	expiresAt := time.Now().UTC().Add(SessionLifetime)
	_, err = s.pool.Exec(ctx, `INSERT INTO auth_sessions(token_hash, user_id, csrf_hash, expires_at)
		VALUES ($1, $2, $3, $4)`, tokenHash[:], user.ID, csrfHash[:], expiresAt)
	if err != nil {
		return LoginResult{}, fmt.Errorf("persist session: %w", err)
	}
	return LoginResult{User: user, Token: token, CSRFToken: csrfToken, ExpiresAt: expiresAt}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (Session, error) {
	if token == "" {
		return Session{}, ErrUnauthenticated
	}
	tokenHash := hashToken(token)
	var session Session
	err := s.pool.QueryRow(ctx, `SELECT u.id, u.email, s.csrf_hash, s.expires_at
		FROM auth_sessions s JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = $1 AND s.expires_at > clock_timestamp()`, tokenHash[:]).
		Scan(&session.User.ID, &session.User.Email, &session.CSRFHash, &session.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrUnauthenticated
	}
	if err != nil {
		return Session{}, fmt.Errorf("load session: %w", err)
	}
	return session, nil
}

// Logout is idempotent: a missing, expired or already revoked token succeeds.
// A live token must present its matching CSRF token before it is revoked.
func (s *Service) Logout(ctx context.Context, token, csrfToken string) error {
	if token == "" {
		return nil
	}
	tokenHash := hashToken(token)
	var csrfHash []byte
	err := s.pool.QueryRow(ctx, `SELECT csrf_hash FROM auth_sessions
		WHERE token_hash = $1 AND expires_at > clock_timestamp()`, tokenHash[:]).Scan(&csrfHash)
	if errors.Is(err, pgx.ErrNoRows) {
		_, deleteErr := s.pool.Exec(ctx, `DELETE FROM auth_sessions WHERE token_hash = $1`, tokenHash[:])
		if deleteErr != nil {
			return fmt.Errorf("clean expired session: %w", deleteErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load session for logout: %w", err)
	}
	if !VerifyCSRF(csrfHash, csrfToken) {
		return ErrCSRF
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM auth_sessions WHERE token_hash = $1`, tokenHash[:])
	if err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	return nil
}

func newToken() (string, [32]byte, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", [32]byte{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw[:])
	return token, hashToken(token), nil
}

func hashToken(token string) [32]byte { return sha256.Sum256([]byte(token)) }

func VerifyCSRF(storedHash []byte, token string) bool {
	if token == "" {
		return false
	}
	providedHash := hashToken(token)
	return subtle.ConstantTimeCompare(storedHash, providedHash[:]) == 1
}
