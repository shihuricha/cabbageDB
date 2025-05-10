package storage

type VersionHashSet = map[Version]struct{}
type TransactionState struct {
	Version  Version
	ReadOnly bool
	Active   VersionHashSet
}

func (txnState *TransactionState) IsVisible(version Version) bool {
	if _, ok := txnState.Active[version]; ok {
		if version == txnState.Version {
			// 虽然活动但是应该对自己可见
			return true
		}
		return false
	} else if txnState.ReadOnly {
		return version < txnState.Version
	} else {
		return version <= txnState.Version
	}
}
