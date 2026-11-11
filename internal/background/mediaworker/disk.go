package mediaworker

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"golang.org/x/sys/unix"
)

const scratchFreeReserveBytes = int64(10 << 30)

type diskAdmission struct {
	mu       sync.Mutex
	root     string
	reserved int64
}

func newDiskAdmission(root string) *diskAdmission { return &diskAdmission{root: root} }

func (d *diskAdmission) Reserve() (func(), bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var stat unix.Statfs_t
	if err := unix.Statfs(d.root, &stat); err != nil {
		return nil, false, fmt.Errorf("inspect media scratch capacity: %w", err)
	}
	if stat.Bsize <= 0 || uint64(stat.Bavail) > math.MaxUint64/uint64(stat.Bsize) {
		return nil, false, errors.New("media scratch capacity is invalid")
	}
	available := uint64(stat.Bavail) * uint64(stat.Bsize)
	needed := uint64(scratchFreeReserveBytes + d.reserved + maxTaskScratchBytes)
	if available < needed {
		return nil, false, nil
	}
	d.reserved += maxTaskScratchBytes
	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			d.reserved -= maxTaskScratchBytes
			d.mu.Unlock()
		})
	}, true, nil
}
