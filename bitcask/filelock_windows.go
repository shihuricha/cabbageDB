//go:build windows

package bitcask

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

func LockFileNonBlocking(file *os.File) error {

	// 使用 LOCKFILE_FAIL_IMMEDIATELY 和 LOCKFILE_EXCLUSIVE_LOCK 标志
	flags := windows.LOCKFILE_FAIL_IMMEDIATELY | windows.LOCKFILE_EXCLUSIVE_LOCK

	err := windows.LockFileEx(windows.Handle(file.Fd()), uint32(flags), 0, 1, 0, &windows.Overlapped{})
	if err != nil {
		return errors.New("file is already locked")
	}

	return err
}
