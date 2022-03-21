package stablestore

type StableStore interface {
	Write([]byte) (int, error)
	WriteAt([]byte, int64) (int, error)
	Sync() error
}
