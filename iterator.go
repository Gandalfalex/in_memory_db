package kv

import "github.com/Gandalfalex/in_memory_db/internal/kvtypes"

// IterOptions configures Iterator. Shared verbatim by DB and FileIndex —
// see internal/kvtypes.IterOptions.
type IterOptions = kvtypes.IterOptions

// Iterator walks keys matching IterOptions.Prefix, for both DB and
// FileIndex — see internal/kvtypes.Iterator.
type Iterator = kvtypes.Iterator
