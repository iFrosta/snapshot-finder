package finder

import (
	"net"
	"sort"
	"strings"
)

// Target is one RPC endpoint to probe for snapshots.
type Target struct {
	Addr    string `json:"addr"` // host:port
	Pubkey  string `json:"pubkey,omitempty"`
	Version string `json:"version,omitempty"`
	Private bool   `json:"private,omitempty"` // derived from the gossip address, not advertised
}

// TargetFilter selects which cluster nodes are probed.
type TargetFilter struct {
	Version         string // exact node version (--version)
	WildcardVersion string // version prefix, e.g. "3.0" (--wildcard_version)
	WithPrivateRPC  bool   // also probe gossip-ip:8899 of nodes without a public RPC
	IPBlacklist     IPSet
}

// IPSet holds "ip" and "ip:port" entries; a bare IP excludes every port.
type IPSet map[string]bool

// ParseIPSet parses a comma/whitespace separated list of "ip" or "ip:port".
func ParseIPSet(lists ...string) IPSet {
	s := IPSet{}
	for _, list := range lists {
		for _, f := range strings.FieldsFunc(list, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' }) {
			s[f] = true
		}
	}
	return s
}

// Has reports whether addr ("ip:port") is blacklisted by ip:port or by ip alone.
func (s IPSet) Has(addr string) bool {
	if s[addr] {
		return true
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && s[host] {
		return true
	}
	return false
}

// TargetStats counts why cluster nodes were not probed.
type TargetStats struct {
	ClusterNodes int `json:"cluster_nodes"`
	ByVersion    int `json:"discarded_by_version"`
	NoRPC        int `json:"no_public_rpc"`
	IPBlacklist  int `json:"discarded_by_ip_blacklist"`
	Targets      int `json:"targets"`
}

// BuildTargets turns getClusterNodes output into a deduplicated, sorted list of
// probe targets. A node without a version is not filtered by version (same as
// upstream).
func BuildTargets(nodes []ClusterNode, f TargetFilter) ([]Target, TargetStats) {
	st := TargetStats{ClusterNodes: len(nodes)}
	seen := map[string]bool{}
	var out []Target
	for _, n := range nodes {
		ver := ""
		if n.Version != nil {
			ver = *n.Version
		}
		if ver != "" {
			if f.Version != "" && ver != f.Version {
				st.ByVersion++
				continue
			}
			// Prefix, not substring: upstream's substring test let "1.18" match "2.1.18".
			if f.WildcardVersion != "" && !strings.HasPrefix(ver, f.WildcardVersion) {
				st.ByVersion++
				continue
			}
		}
		t := Target{Pubkey: n.Pubkey, Version: ver}
		switch {
		case n.RPC != nil && *n.RPC != "":
			t.Addr = *n.RPC
		case f.WithPrivateRPC && n.Gossip != nil && *n.Gossip != "":
			host, _, err := net.SplitHostPort(*n.Gossip)
			if err != nil {
				st.NoRPC++
				continue
			}
			t.Addr = net.JoinHostPort(host, "8899")
			t.Private = true
		default:
			st.NoRPC++
			continue
		}
		if seen[t.Addr] {
			continue
		}
		if f.IPBlacklist.Has(t.Addr) {
			st.IPBlacklist++
			continue
		}
		seen[t.Addr] = true
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	st.Targets = len(out)
	return out, st
}
