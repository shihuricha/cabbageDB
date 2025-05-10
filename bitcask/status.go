package bitcask

type Status struct {
	Name            string
	Keys            uint64
	Size            uint64
	TotalDiskSize   uint64
	LiveDiskSize    uint64
	GarbageDiskSize uint64
	FileName        string
}
