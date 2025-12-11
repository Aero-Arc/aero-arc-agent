package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Identity represents the stable identity of an AeroArc Agent instance.
//
// The resolver makes a best-effort attempt to populate as many fields as
// possible from hardware, OS, and persisted identifiers without ever
// panicking or logging.
type Identity struct {
	GeneratedID string
	FinalID     string
}

var cachedIdentity Identity
var once sync.Once

// resolverEnv encapsulates side-effectful operations so they can be swapped
// out in tests without relying on global state.
type resolverEnv struct {
	readFile    func(path string) ([]byte, error)
	interfaces  func() ([]net.Interface, error)
	userHomeDir func() (string, error)
	mkdirAll    func(path string, perm os.FileMode) error
	writeFile   func(name string, data []byte, perm os.FileMode) error
}

// defaultEnv constructs the resolver environment backed by the standard
// library for use in production.
func defaultEnv() resolverEnv {
	return resolverEnv{
		readFile:    os.ReadFile,
		interfaces:  net.Interfaces,
		userHomeDir: os.UserHomeDir,
		mkdirAll:    os.MkdirAll,
		writeFile:   os.WriteFile,
	}
}

// Resolve computes the current process identity using a combination of
// hardware identifiers, OS machine identifiers, MAC address and a persisted
// fallback UUID. It never panics and returns a zero-valued Identity on
// catastrophic failure.
func Resolve() Identity {
	once.Do(func() {
		cachedIdentity = resolveWithEnv(defaultEnv())
	})

	return cachedIdentity
}

// resolveWithEnv performs resolution using the supplied environment. It is
// internal so that tests can exercise the full resolution logic with mocked
// dependencies.
func resolveWithEnv(env resolverEnv) Identity {
	generatedID := resolveGeneratedID(env)

	finalID := computeFinalID(generatedID)

	return Identity{
		GeneratedID: generatedID,
		FinalID:     finalID,
	}
}

// resolveGeneratedID returns a persisted UUID stored at
// ~/.aeroarc/agent-id, creating it if necessary. Errors are ignored and
// result in an empty string.
func resolveGeneratedID(env resolverEnv) string {
	home, err := env.userHomeDir()
	if err != nil || home == "" {
		return ""
	}

	path := filepath.Join(home, ".aeroarc", "agent-id")
	// Attempt to load an existing ID.
	if data, err := env.readFile(path); err == nil {
		if id := normalizeID(string(data)); id != "" {
			return id
		}
	}

	// Generate a fresh UUIDv4.
	uuid, err := generateUUIDv4()
	if err != nil {
		return ""
	}

	dir := filepath.Dir(path)
	if dir != "" {
		_ = env.mkdirAll(dir, 0o700)
	}

	_ = env.writeFile(path, []byte(uuid), 0o600)

	return uuid
}

// normalizeID trims whitespace and NUL characters from the provided string.
func normalizeID(s string) string {
	// Remove NUL from both ends, then whitespace.
	s = strings.Trim(s, "\x00")
	s = strings.TrimSpace(s)
	return s
}

// computeFinalID constructs the composite identity digest as:
// sha256(UUIDv4), hex-encoded.
func computeFinalID(generatedID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(generatedID))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// generateUUIDv4 produces an RFC 4122 version 4 UUID string using
// crypto/rand. Errors are returned rather than panicking.
// 1 in a billion chance of collision after 103 trillion UUIDs
// https://en.wikipedia.org/wiki/Universally_unique_identifier#:~:text=Thus%2C%20the%20probability%20to%20find,later%20in%20the%20manufacturing%20process.
func generateUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	// Set version (4) and variant (RFC 4122).
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	hexBytes := hex.EncodeToString(b[:])
	uuid := fmt.Sprintf("%s-%s-%s-%s-%s",
		hexBytes[0:8],
		hexBytes[8:12],
		hexBytes[12:16],
		hexBytes[16:20],
		hexBytes[20:32],
	)

	return uuid, nil
}
