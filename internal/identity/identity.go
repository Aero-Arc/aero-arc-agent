package identity

import (
	"bytes"
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
	HardwareID  string
	MachineID   string
	MACAddress  string
	GeneratedID string
	FinalID     string
	Method      string
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
	hardwareID := resolveHardwareID(env)
	machineID := resolveMachineID(env)
	macAddress := resolveMACAddress(env)

	var generatedID string
	if hardwareID == "" && machineID == "" {
		generatedID = resolveGeneratedID(env)
	}

	finalID := computeFinalID(hardwareID, machineID, macAddress, generatedID)
	method := determineMethod(hardwareID, machineID, generatedID)

	return Identity{
		HardwareID:  hardwareID,
		MachineID:   machineID,
		MACAddress:  macAddress,
		GeneratedID: generatedID,
		FinalID:     finalID,
		Method:      method,
	}
}

// resolveHardwareID attempts to gather a stable hardware-anchored identifier
// from a variety of platform-specific sources.
func resolveHardwareID(env resolverEnv) string {
	// 1. Device-tree serials (common on embedded boards).
	if id := readDeviceTreeSerial(env, "/proc/device-tree/serial-number"); id != "" {
		return id
	}
	if id := readDeviceTreeSerial(env, "/sys/firmware/devicetree/base/serial-number"); id != "" {
		return id
	}

	// 2. Raspberry Pi CPU serial from /proc/cpuinfo.
	if id := readRaspberryPiSerial(env); id != "" {
		return id
	}

	// 3. Jetson UID.
	if id := readIDFromFile(env, "/sys/module/tegra_fuse/parameters/tegra_chip_uid", false); id != "" {
		return id
	}

	// 4. eMMC CID.
	if id := readIDFromFile(env, "/sys/block/mmcblk0/device/cid", false); id != "" {
		return id
	}

	// 5. DMI UUID.
	if id := readIDFromFile(env, "/sys/class/dmi/id/product_uuid", false); id != "" {
		return id
	}

	return ""
}

// resolveMachineID reads the OS-provided machine-id, if any.
func resolveMachineID(env resolverEnv) string {
	id := readIDFromFile(env, "/etc/machine-id", false)
	return id
}

// resolveMACAddress returns the MAC address of the first non-loopback
// interface that has a hardware address.
func resolveMACAddress(env resolverEnv) string {
	ifaces, err := env.interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if len(iface.HardwareAddr) == 0 {
			continue
		}
		return iface.HardwareAddr.String()
	}

	return ""
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

// readDeviceTreeSerial reads a device-tree serial-number file, stripping
// trailing NUL terminators and whitespace as required on ARM platforms.
func readDeviceTreeSerial(env resolverEnv, path string) string {
	return readIDFromFile(env, path, true)
}

// readRaspberryPiSerial parses the CPU serial from /proc/cpuinfo on
// Raspberry Pi systems.
func readRaspberryPiSerial(env resolverEnv) string {
	data, err := env.readFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Serial") {
			continue
		}

		// Format is typically: "Serial  : 00000000abcdef01"
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		serial := normalizeID(parts[1])
		if serial != "" {
			return serial
		}
	}

	return ""
}

// readIDFromFile reads the contents of a file and returns a normalized
// identifier string, optionally stripping NUL bytes which are common in
// device-tree blobs.
func readIDFromFile(env resolverEnv, path string, stripNull bool) string {
	data, err := env.readFile(path)
	if err != nil {
		return ""
	}
	if stripNull {
		// Device-tree files often contain NUL padding; strip all NUL bytes.
		data = bytes.ReplaceAll(data, []byte{0x00}, nil)
	}

	return normalizeID(string(data))
}

// normalizeID trims whitespace and NUL characters from the provided string.
func normalizeID(s string) string {
	// Remove NUL from both ends, then whitespace.
	s = strings.Trim(s, "\x00")
	s = strings.TrimSpace(s)
	return s
}

// computeFinalID constructs the composite identity digest as:
// sha256(hardwareID + machineID + macAddress + generatedID), hex-encoded.
func computeFinalID(hardwareID, machineID, macAddress, generatedID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(hardwareID))
	_, _ = h.Write([]byte(machineID))
	_, _ = h.Write([]byte(macAddress))
	_, _ = h.Write([]byte(generatedID))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

// determineMethod returns the resolution method string based on which
// identifiers were actually discovered.
func determineMethod(hardwareID, machineID, generatedID string) string {
	switch {
	case hardwareID != "":
		return "hardware"
	case machineID != "":
		return "machine-id"
	case generatedID != "":
		return "generated"
	default:
		return ""
	}
}

// generateUUIDv4 produces an RFC 4122 version 4 UUID string using
// crypto/rand. Errors are returned rather than panicking.
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
