package kvtypes

// KeyFunc extracts the index key from one line of a FileIndex's file (or
// a SortedIndex source file — both engines share this exact contract),
// without necessarily unmarshalling the whole line (for JSONL, see
// fileindex.JSONStringKey). Returning ok=false skips the line: that is
// how blank, malformed, and torn-tail lines are excluded during a rebuild
// scan, so the file needs no CRC framing. It must be deterministic — Put
// derives the key it indexes under by calling it on the new line, and
// reopen must derive the same key from the same bytes.
type KeyFunc func(line []byte) (key []byte, ok bool)
