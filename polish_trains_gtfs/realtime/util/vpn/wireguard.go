// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package vpn

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"gopkg.in/ini.v1"
)

var DefaultDNS = "1.1.1.1"

type WireguardConfig struct {
	Endpoint     string
	DNS          netip.Addr
	Address      netip.Addr
	PublicKey    string
	PrivateKey   string
	PreSharedKey string
}

func LoadWireguardConfigFromFile(path string) (*WireguardConfig, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, err
	}

	c := new(WireguardConfig)

	c.DNS, err = netip.ParseAddr(cfg.Section("Interface").Key("DNS").MustString(DefaultDNS))
	if err != nil {
		return nil, fmt.Errorf("invalid dns address: %w", err)
	}

	c.Address, err = netip.ParseAddr(stripCIDR(cfg.Section("Interface").Key("Address").String()))
	if err != nil {
		return nil, fmt.Errorf("invalid interface address: %w", err)
	}

	c.Endpoint = cfg.Section("Peer").Key("Endpoint").String()
	c.PublicKey = cfg.Section("Peer").Key("PublicKey").String()
	c.PrivateKey = cfg.Section("Interface").Key("PrivateKey").String()
	c.PreSharedKey = cfg.Section("Peer").Key("PresharedKey").String()
	return c, nil
}

func (c *WireguardConfig) ResolveEndpoint() string {
	host, port, err := net.SplitHostPort(c.Endpoint)
	if err != nil {
		panic(fmt.Errorf("invalid endpoint %s: %w", c.Endpoint, err))
	}

	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		panic(fmt.Errorf("could not resolve endpoint %s: %w", c.Endpoint, err))
	}

	return net.JoinHostPort(ips[0].String(), port)
}

func (c *WireguardConfig) AsUAPIString() string {
	b := &strings.Builder{}

	b.WriteString("private_key=")
	b.WriteString(b64toHex(c.PrivateKey))
	b.WriteByte('\n')

	b.WriteString("public_key=")
	b.WriteString(b64toHex(c.PublicKey))
	b.WriteByte('\n')

	if c.PreSharedKey != "" {
		b.WriteString("preshared_key=")
		b.WriteString(b64toHex(c.PreSharedKey))
		b.WriteByte('\n')
	}

	b.WriteString("endpoint=")
	b.WriteString(c.ResolveEndpoint())
	b.WriteByte('\n')

	b.WriteString("allowed_ip=0.0.0.0/0\n")
	b.WriteString("allowed_ip=::/0\n")

	return b.String()
}

func b64toHex(x string) string {
	decoded, _ := base64.StdEncoding.DecodeString(x)
	return hex.EncodeToString(decoded)
}

func stripCIDR(x string) string {
	x, _, _ = strings.Cut(x, "/")
	return x
}

func NewWireguardClient(config *WireguardConfig) (c *http.Client, closer func(), err error) {
	tun, tnet, err := netstack.CreateNetTUN([]netip.Addr{config.Address}, []netip.Addr{config.DNS}, 1420)
	if err != nil {
		return nil, nil, err
	}

	dev := device.NewDevice(tun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelError, ""))
	err = dev.IpcSet(config.AsUAPIString())
	if err != nil {
		return nil, nil, err
	}

	err = dev.Up()
	if err != nil {
		return nil, nil, err
	}

	c = &http.Client{Transport: &http.Transport{DialContext: tnet.DialContext}}
	closer = func() { dev.Close() }
	return c, closer, nil
}
