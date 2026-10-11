package finder

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Kind tells a full snapshot archive from an incremental one.
type Kind int

const (
	KindFull Kind = iota
	KindIncremental
)

func (k Kind) String() string {
	if k == KindIncremental {
		return "incremental"
	}
	return "full"
}

// Archive is a parsed snapshot archive file name, as served by an Agave RPC
// node behind the /snapshot.tar.bz2 and /incremental-snapshot.tar.bz2 redirects:
//
//	snapshot-<slot>-<hash>.tar.zst
//	incremental-snapshot-<base slot>-<slot>-<hash>.tar.zst
type Archive struct {
	Name     string `json:"name"`
	Kind     Kind   `json:"-"`
	BaseSlot uint64 `json:"base_slot,omitempty"` // incremental only: slot of the full snapshot it applies to
	Slot     uint64 `json:"slot"`
	Hash     string `json:"hash"`
	Ext      string `json:"-"` // "tar.zst", "tar.bz2", ... or "tar"
}

// Compressed reports whether the archive is compressed. Uncompressed .tar
// archives are several times larger and are skipped, as upstream did.
func (a Archive) Compressed() bool { return a.Ext != "tar" }

// Base58 alphabet, 32..44 chars: the encoding of a 32-byte hash.
const hashPattern = `[1-9A-HJ-NP-Za-km-z]{32,44}`
const extPattern = `(tar|tar\.zst|tar\.bz2|tar\.gz|tar\.lz4|tar\.xz)`

var (
	fullRe = regexp.MustCompile(`^snapshot-([0-9]{1,20})-(` + hashPattern + `)\.` + extPattern + `$`)
	incRe  = regexp.MustCompile(`^incremental-snapshot-([0-9]{1,20})-([0-9]{1,20})-(` + hashPattern + `)\.` + extPattern + `$`)
)

// ParseArchiveName parses a bare archive file name. Anything that is not
// exactly a snapshot archive name — including names with path separators,
// ".." or a "tmp-" prefix — is rejected, so the result is always safe to use
// as a file name inside the snapshot directory.
func ParseArchiveName(name string) (Archive, error) {
	if m := fullRe.FindStringSubmatch(name); m != nil {
		slot, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return Archive{}, fmt.Errorf("bad slot in %q: %w", name, err)
		}
		return Archive{Name: name, Kind: KindFull, Slot: slot, Hash: m[2], Ext: m[3]}, nil
	}
	if m := incRe.FindStringSubmatch(name); m != nil {
		base, err1 := strconv.ParseUint(m[1], 10, 64)
		slot, err2 := strconv.ParseUint(m[2], 10, 64)
		if err1 != nil || err2 != nil {
			return Archive{}, fmt.Errorf("bad slot in %q", name)
		}
		if slot <= base {
			return Archive{}, fmt.Errorf("incremental slot %d is not after its base %d in %q", slot, base, name)
		}
		return Archive{Name: name, Kind: KindIncremental, BaseSlot: base, Slot: slot, Hash: m[3], Ext: m[4]}, nil
	}
	return Archive{}, fmt.Errorf("not a snapshot archive name: %q", name)
}

// ParseLocation validates the Location header of a snapshot redirect. Only a
// same-host absolute path ("/<archive name>") is accepted: a redirect to
// another host, a query string or a path with directories is refused.
func ParseLocation(loc string, want Kind) (Archive, error) {
	if !strings.HasPrefix(loc, "/") || strings.HasPrefix(loc, "//") {
		return Archive{}, fmt.Errorf("location is not a same-host path: %q", loc)
	}
	a, err := ParseArchiveName(loc[1:])
	if err != nil {
		return Archive{}, err
	}
	if a.Kind != want {
		return Archive{}, fmt.Errorf("expected a %s snapshot, got %q", want, loc)
	}
	return a, nil
}

// LocalArchives lists complete snapshot archives in dir (temporary "tmp-*"
// files are never matched), full ones sorted by slot, newest first.
func LocalArchives(dir string) (fulls, incs []Archive, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		a, err := ParseArchiveName(e.Name())
		if err != nil {
			continue
		}
		if a.Kind == KindFull {
			fulls = append(fulls, a)
		} else {
			incs = append(incs, a)
		}
	}
	sort.Slice(fulls, func(i, j int) bool { return fulls[i].Slot > fulls[j].Slot })
	sort.Slice(incs, func(i, j int) bool { return incs[i].Slot > incs[j].Slot })
	return fulls, incs, nil
}

func archivePath(dir string, a Archive) string { return filepath.Join(dir, a.Name) }
func tmpPath(dir string, a Archive) string     { return filepath.Join(dir, "tmp-"+a.Name) }
