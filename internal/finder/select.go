package finder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Candidate is a node we could download a full snapshot from.
type Candidate struct {
	Addr      string        `json:"snapshot_address"`
	Latency   time.Duration `json:"-"`
	Full      Archive       `json:"full"`
	Inc       *Archive      `json:"incremental,omitempty"` // this node's own incremental, only if based on Full
	BestSlot  uint64        `json:"best_slot"`             // newest slot reachable: Full + the newest matching incremental cluster-wide
	SlotsDiff int64         `json:"slots_diff"`            // current slot - BestSlot
	Agreement int           `json:"agreement"`             // nodes serving the same full (slot, hash)
	Version   string        `json:"version,omitempty"`
}

// SelectOptions are the filters applied to probe results.
type SelectOptions struct {
	CurrentSlot  uint64
	MaxAge       uint64 // slots
	SpecificSlot uint64 // 0 = any
	MaxLatency   time.Duration
	Blacklist    []string // slot numbers or (sub)strings of archive names/hashes
	SortOrder    string   // "latency" (default) or "slots_diff"
	MinAgreement int
}

// futureSlotTolerance: a snapshot more than this many slots *ahead* of the
// cluster's slot is bogus (upstream used the same -100).
const futureSlotTolerance = 100

// Blacklisted reports whether an archive matches a --blacklist entry. An
// all-digit entry must equal one of the archive's slots; anything else is a
// substring of the name (typically a hash).
func Blacklisted(a Archive, entries []string) bool {
	for _, e := range entries {
		if e == "" {
			continue
		}
		if n, err := strconv.ParseUint(e, 10, 64); err == nil {
			if a.Slot == n || (a.Kind == KindIncremental && a.BaseSlot == n) {
				return true
			}
			continue
		}
		if strings.Contains(a.Name, e) {
			return true
		}
	}
	return false
}

type hashKey struct {
	base, slot uint64
}

// majorityHashes returns, for each (base, slot), the hash served by a strict
// plurality of nodes, and whether the slot was contested. On a tie no hash
// wins: two different archives claiming the same slot means somebody is
// lying and we cannot tell who.
func majorityHashes(archives []*Archive) (winner map[hashKey]string, count map[hashKey]map[string]int) {
	count = map[hashKey]map[string]int{}
	for _, a := range archives {
		if a == nil {
			continue
		}
		k := hashKey{a.BaseSlot, a.Slot}
		if count[k] == nil {
			count[k] = map[string]int{}
		}
		count[k][a.Hash]++
	}
	winner = map[hashKey]string{}
	for k, hashes := range count {
		best, bestN, tie := "", 0, false
		for h, n := range hashes {
			switch {
			case n > bestN:
				best, bestN, tie = h, n, false
			case n == bestN:
				tie = true
			}
		}
		if !tie {
			winner[k] = best
		}
	}
	return winner, count
}

// Selection is the outcome of SelectCandidates.
type Selection struct {
	Candidates []Candidate
	Discarded  map[string]int
	// BestInc maps a full snapshot slot to the newest agreed incremental for it.
	BestInc map[uint64]Archive
	// Warnings worth showing to the operator (hash conflicts).
	Warnings []string
}

// SelectCandidates filters and ranks probe results.
func SelectCandidates(results []ProbeResult, o SelectOptions) Selection {
	sel := Selection{Discarded: map[string]int{}, BestInc: map[uint64]Archive{}}
	if o.MinAgreement < 1 {
		o.MinAgreement = 1
	}

	var fulls, incs []*Archive
	for i := range results {
		r := &results[i]
		if r.Err != nil {
			sel.Discarded[r.Reason]++
			continue
		}
		fulls = append(fulls, r.Full)
		if r.Inc != nil {
			incs = append(incs, r.Inc)
		}
	}
	fullWinner, fullCount := majorityHashes(fulls)
	incWinner, incCount := majorityHashes(incs)
	for k, hashes := range fullCount {
		if len(hashes) > 1 {
			sel.Warnings = append(sel.Warnings, fmt.Sprintf("full snapshot slot %d is served with %d different hashes %v; keeping %q", k.slot, len(hashes), hashes, fullWinner[k]))
		}
	}
	for k, hashes := range incCount {
		if len(hashes) > 1 {
			sel.Warnings = append(sel.Warnings, fmt.Sprintf("incremental snapshot %d->%d is served with %d different hashes %v; keeping %q", k.base, k.slot, len(hashes), hashes, incWinner[k]))
		}
	}
	sort.Strings(sel.Warnings)

	// Newest agreed incremental per base slot. Latency does not matter for
	// these: they are small. An incremental's name does not carry the hash of
	// the full it builds on, so it only counts when the same node serves the
	// agreed full for its base — otherwise a node with a conflicting full could
	// still win "freshest incremental".
	for i := range results {
		r := &results[i]
		if r.Err != nil || r.Inc == nil {
			continue
		}
		a := r.Inc
		if r.Full.Slot != a.BaseSlot || fullWinner[hashKey{0, r.Full.Slot}] != r.Full.Hash {
			continue
		}
		if !a.Compressed() || Blacklisted(*a, o.Blacklist) || incWinner[hashKey{a.BaseSlot, a.Slot}] != a.Hash {
			continue
		}
		if a.Slot > o.CurrentSlot+futureSlotTolerance {
			continue
		}
		if cur, ok := sel.BestInc[a.BaseSlot]; !ok || a.Slot > cur.Slot {
			sel.BestInc[a.BaseSlot] = *a
		}
	}

	for i := range results {
		r := &results[i]
		if r.Err != nil {
			continue
		}
		f := *r.Full
		switch {
		case !f.Compressed():
			sel.Discarded["uncompressed"]++
			continue
		case fullWinner[hashKey{0, f.Slot}] != f.Hash:
			sel.Discarded["hash_conflict"]++
			continue
		case Blacklisted(f, o.Blacklist):
			sel.Discarded["blacklisted"]++
			continue
		case o.MaxLatency > 0 && r.Latency > o.MaxLatency:
			sel.Discarded["latency"]++
			continue
		}
		agreement := fullCount[hashKey{0, f.Slot}][f.Hash]
		if agreement < o.MinAgreement {
			sel.Discarded["low_agreement"]++
			continue
		}
		c := Candidate{Addr: r.Addr, Latency: r.Latency, Full: f, BestSlot: f.Slot, Agreement: agreement, Version: r.Version}
		if r.Inc != nil && r.Inc.BaseSlot == f.Slot && r.Inc.Compressed() &&
			incWinner[hashKey{r.Inc.BaseSlot, r.Inc.Slot}] == r.Inc.Hash && !Blacklisted(*r.Inc, o.Blacklist) {
			inc := *r.Inc
			c.Inc = &inc
		}
		if best, ok := sel.BestInc[f.Slot]; ok && best.Slot > c.BestSlot {
			c.BestSlot = best.Slot
		}
		if o.SpecificSlot != 0 {
			// Network restart: the full snapshot at exactly that slot, or a
			// full whose incremental ends exactly there.
			switch {
			case f.Slot == o.SpecificSlot:
				c.Inc, c.BestSlot = nil, f.Slot
			case c.Inc != nil && c.Inc.Slot == o.SpecificSlot:
				c.BestSlot = c.Inc.Slot
			default:
				sel.Discarded["slot"]++
				continue
			}
		}
		c.SlotsDiff = int64(o.CurrentSlot) - int64(c.BestSlot)
		if o.SpecificSlot == 0 {
			if c.SlotsDiff < -futureSlotTolerance {
				sel.Discarded["future_slot"]++
				continue
			}
			if c.SlotsDiff > int64(o.MaxAge) {
				sel.Discarded["stale"]++
				continue
			}
		}
		sel.Candidates = append(sel.Candidates, c)
	}

	sort.SliceStable(sel.Candidates, func(i, j int) bool {
		a, b := sel.Candidates[i], sel.Candidates[j]
		if o.SortOrder == "slots_diff" && a.SlotsDiff != b.SlotsDiff {
			return a.SlotsDiff < b.SlotsDiff
		}
		if a.Latency != b.Latency {
			return a.Latency < b.Latency
		}
		return a.SlotsDiff < b.SlotsDiff
	})
	return sel
}

// IncSource is a node serving an incremental snapshot for a given base.
type IncSource struct {
	Addr    string
	Inc     Archive
	Latency time.Duration
}

// IncrementalSources lists nodes serving an agreed incremental for the full
// snapshot `full`, newest slot first, then lowest latency. Only nodes that
// serve that very full snapshot (slot and hash) are trusted for its
// incremental. Only the newest slot is useful; older ones are fallbacks.
func IncrementalSources(results []ProbeResult, full Archive, o SelectOptions) []IncSource {
	base := full.Slot
	var incs []*Archive
	for i := range results {
		if results[i].Err == nil && results[i].Inc != nil {
			incs = append(incs, results[i].Inc)
		}
	}
	winner, _ := majorityHashes(incs)
	var out []IncSource
	for i := range results {
		r := &results[i]
		if r.Err != nil || r.Inc == nil {
			continue
		}
		if r.Full == nil || r.Full.Slot != full.Slot || r.Full.Hash != full.Hash {
			continue
		}
		a := *r.Inc
		if a.BaseSlot != base || !a.Compressed() || Blacklisted(a, o.Blacklist) || winner[hashKey{a.BaseSlot, a.Slot}] != a.Hash {
			continue
		}
		if a.Slot > o.CurrentSlot+futureSlotTolerance {
			continue
		}
		if o.SpecificSlot != 0 && a.Slot != o.SpecificSlot {
			continue
		}
		out = append(out, IncSource{Addr: r.Addr, Inc: a, Latency: r.Latency})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Inc.Slot != out[j].Inc.Slot {
			return out[i].Inc.Slot > out[j].Inc.Slot
		}
		return out[i].Latency < out[j].Latency
	})
	return out
}
