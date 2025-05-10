//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package bitcask

import (
	"errors"
	"os"
	"syscall"
)

func LockFileNonBlocking(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("file is already locked")
	}
	return nil
}
