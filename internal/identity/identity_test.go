package identity

import (
	"bytes"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// helper to build a resolverEnv with only file contents mocked.
func newFileEnv(files map[string]string) resolverEnv {
	return resolverEnv{
		readFile: func(path string) ([]byte, error) {
			if v, ok := files[path]; ok {
				return []byte(v), nil
			}
			return nil, os.ErrNotExist
		},
		interfaces: func() ([]net.Interface, error) {
			return nil, nil
		},
		userHomeDir: func() (string, error) {
			return "", os.ErrNotExist
		},
		mkdirAll: func(path string, perm os.FileMode) error {
			return nil
		},
		writeFile: func(name string, data []byte, perm os.FileMode) error {
			return nil
		},
	}
}

func TestReadDeviceTreeSerialStripsNullsAndWhitespace(t *testing.T) {
	files := map[string]string{
		"/proc/device-tree/serial-number": "ABC123\x00\x00\n",
	}
	env := newFileEnv(files)

	id := readDeviceTreeSerial(env, "/proc/device-tree/serial-number")
	if id != "ABC123" {
		t.Fatalf("expected 'ABC123', got %q", id)
	}
}

func TestReadRaspberryPiSerialParsing(t *testing.T) {
	const cpuinfo = `
processor	: 0
model name	: ARMv7 Processor rev 4 (v7l)
Serial		: 00000000deadbeef
`
	env := newFileEnv(map[string]string{
		"/proc/cpuinfo": cpuinfo,
	})

	id := readRaspberryPiSerial(env)
	if id != "00000000deadbeef" {
		t.Fatalf("expected Raspberry Pi serial '00000000deadbeef', got %q", id)
	}
}

func TestResolveHardwareIDFallbackOrder(t *testing.T) {
	files := map[string]string{
		// First path missing.
		"/sys/firmware/devicetree/base/serial-number": "DTREE123",
	}
	env := newFileEnv(files)

	id := resolveHardwareID(env)
	if id != "DTREE123" {
		t.Fatalf("expected hardware ID from second device-tree path, got %q", id)
	}
}

func TestResolveMachineIDTrimmed(t *testing.T) {
	env := newFileEnv(map[string]string{
		"/etc/machine-id": "  machine-id-xyz \n",
	})

	id := resolveMachineID(env)
	if id != "machine-id-xyz" {
		t.Fatalf("expected trimmed machine-id 'machine-id-xyz', got %q", id)
	}
}

func TestResolveMACAddressSkipsLoopback(t *testing.T) {
	env := resolverEnv{
		readFile: func(path string) ([]byte, error) {
			return nil, os.ErrNotExist
		},
		interfaces: func() ([]net.Interface, error) {
			return []net.Interface{
				{
					Name:         "lo",
					Flags:        net.FlagLoopback,
					HardwareAddr: net.HardwareAddr{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				},
				{
					Name:         "eth0",
					Flags:        0,
					HardwareAddr: net.HardwareAddr{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01},
				},
			}, nil
		},
		userHomeDir: func() (string, error) {
			return "", os.ErrNotExist
		},
		mkdirAll: func(path string, perm os.FileMode) error {
			return nil
		},
		writeFile: func(name string, data []byte, perm os.FileMode) error {
			return nil
		},
	}

	mac := resolveMACAddress(env)
	if mac != "de:ad:be:ef:00:01" {
		t.Fatalf("expected eth0 MAC 'de:ad:be:ef:00:01', got %q", mac)
	}
}

func TestCompositeHashStability(t *testing.T) {
	env := resolverEnv{
		readFile: func(path string) ([]byte, error) {
			switch path {
			case "/proc/device-tree/serial-number":
				return []byte("HWID123"), nil
			case "/etc/machine-id":
				return []byte("MID456"), nil
			default:
				return nil, os.ErrNotExist
			}
		},
		interfaces: func() ([]net.Interface, error) {
			return []net.Interface{
				{
					Name:         "eth0",
					HardwareAddr: net.HardwareAddr{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
				},
			}, nil
		},
		userHomeDir: func() (string, error) {
			return "", os.ErrNotExist
		},
		mkdirAll: func(path string, perm os.FileMode) error {
			return nil
		},
		writeFile: func(name string, data []byte, perm os.FileMode) error {
			return nil
		},
	}

	id1 := resolveWithEnv(env)
	id2 := resolveWithEnv(env)

	if id1.FinalID != id2.FinalID {
		t.Fatalf("expected stable final ID, got %q and %q", id1.FinalID, id2.FinalID)
	}

	// Also verify the hash matches our direct computation of the same inputs.
	expectedBytes := sha256.Sum256([]byte("HWID123" + "MID456" + "aa:bb:cc:dd:ee:ff"))
	expected := strings.ToLower(hexEncode(expectedBytes[:]))
	if id1.FinalID != expected {
		t.Fatalf("expected final ID %q, got %q", expected, id1.FinalID)
	}
}

func hexEncode(b []byte) string {
	const hexChars = "0123456789abcdef"
	var buf bytes.Buffer
	for _, v := range b {
		buf.WriteByte(hexChars[v>>4])
		buf.WriteByte(hexChars[v&0x0f])
	}
	return buf.String()
}

func TestGeneratedIDPersistence(t *testing.T) {
	tempHome := t.TempDir()

	var writtenPath string
	var writtenData []byte

	env := resolverEnv{
		readFile: func(path string) ([]byte, error) {
			// Only allow reads from the generated ID path; simulate missing on first call.
			if path == filepath.Join(tempHome, ".aeroarc", "agent-id") {
				if writtenData != nil {
					return writtenData, nil
				}
				return nil, os.ErrNotExist
			}
			return nil, os.ErrNotExist
		},
		interfaces: func() ([]net.Interface, error) {
			return nil, nil
		},
		userHomeDir: func() (string, error) {
			return tempHome, nil
		},
		mkdirAll: func(path string, perm os.FileMode) error {
			return os.MkdirAll(path, perm)
		},
		writeFile: func(name string, data []byte, perm os.FileMode) error {
			writtenPath = name
			writtenData = append([]byte(nil), data...)
			return os.WriteFile(name, data, perm)
		},
	}

	id1 := resolveWithEnv(env)
	if id1.GeneratedID == "" {
		t.Fatalf("expected generated ID to be set")
	}
	if method := determineMethod("", "", id1.GeneratedID); method != "generated" {
		t.Fatalf("expected method 'generated', got %q", method)
	}

	if writtenPath == "" {
		t.Fatalf("expected generated ID to be written to disk")
	}

	// Second resolution should load the same ID from disk.
	id2 := resolveWithEnv(env)
	if id2.GeneratedID != id1.GeneratedID {
		t.Fatalf("expected persisted generated ID %q, got %q", id1.GeneratedID, id2.GeneratedID)
	}
}

func TestResolveMethodSelection(t *testing.T) {
	cases := []struct {
		hw, mid, gid string
		expected     string
	}{
		{hw: "hw", mid: "mid", gid: "gid", expected: "hardware"},
		{hw: "hw", mid: "", gid: "", expected: "hardware"},
		{hw: "", mid: "mid", gid: "gid", expected: "machine-id"},
		{hw: "", mid: "", gid: "gid", expected: "generated"},
		{hw: "", mid: "", gid: "", expected: ""},
	}

	for _, tc := range cases {
		method := determineMethod(tc.hw, tc.mid, tc.gid)
		if method != tc.expected {
			t.Fatalf("determineMethod(%q,%q,%q) = %q, expected %q",
				tc.hw, tc.mid, tc.gid, method, tc.expected)
		}
	}
}

func TestResolveUsesHardwarePreferredOverMachineID(t *testing.T) {
	env := resolverEnv{
		readFile: func(path string) ([]byte, error) {
			switch path {
			case "/proc/device-tree/serial-number":
				return []byte("HWID123"), nil
			case "/etc/machine-id":
				return []byte("MID456"), nil
			default:
				return nil, os.ErrNotExist
			}
		},
		interfaces: func() ([]net.Interface, error) {
			return nil, nil
		},
		userHomeDir: func() (string, error) {
			return "", os.ErrNotExist
		},
		mkdirAll: func(path string, perm os.FileMode) error {
			return nil
		},
		writeFile: func(name string, data []byte, perm os.FileMode) error {
			return nil
		},
	}

	id := resolveWithEnv(env)
	if id.HardwareID != "HWID123" {
		t.Fatalf("expected hardware ID 'HWID123', got %q", id.HardwareID)
	}
	if id.Method != "hardware" {
		t.Fatalf("expected method 'hardware', got %q", id.Method)
	}
	if id.GeneratedID != "" {
		t.Fatalf("expected no generated ID when hardware or machine-id is present")
	}
}

func TestResolvePublicFunctionDoesNotPanic(t *testing.T) {
	// This is a smoke test; if Resolve panics, the test will fail.
	_ = Resolve()
}


