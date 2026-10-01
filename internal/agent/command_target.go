package agent

import (
	"fmt"
	"path/filepath"
	"strings"
)

func (a *Agent) commandTargetIdentity(target *mavlinkTarget) string {
	if target == nil || target.channel == nil {
		return ""
	}
	if a.options == nil {
		return ""
	}
	endpoint := ""
	if a.options.Debug {
		address := strings.TrimSpace(a.options.DebugMAVLinkAddress)
		if address == "" {
			address = "0.0.0.0:14550"
		}
		endpoint = "udp-server:" + address
	} else {
		if a.options.SerialPath == "" {
			return ""
		}
		path, err := filepath.Abs(a.options.SerialPath)
		if err != nil {
			return ""
		}
		endpoint = "serial:" + path
	}
	return fmt.Sprintf("%s/%d/%d/%d/%d", endpoint, target.systemID, target.componentID, target.vehicleType, target.autopilot)
}
