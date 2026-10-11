//go:build !(linux || darwin || freebsd)

package finder

import "errors"

func freeSpace(string) (uint64, error) {
	return 0, errors.New("free space check not supported on this OS")
}
