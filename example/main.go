// Command example is a minimal, runnable demonstration of the kv package:
// open a DB, Put/Get/Delete a few keys, iterate by prefix (raw and typed),
// inspect stats, and close.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	kv "github.com/Gandalfalex/in_memory_db"
)

type user struct {
	Name string
	Age  int
}

func main() {
	dir, err := os.MkdirTemp("", "kv-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := kv.Open(kv.DefaultOptions(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	must(db.Put([]byte("user:1"), []byte("alice")))
	must(db.Put([]byte("user:2"), []byte("bob")))
	must(db.Put([]byte("order:1"), []byte("widget")))

	v, err := db.Get([]byte("user:1"))
	must(err)
	fmt.Printf("user:1 = %s\n", v)

	fmt.Println("keys with prefix \"user:\" (range-over-func):")
	for k, v := range db.All(kv.IterOptions{Prefix: []byte("user:")}) {
		fmt.Printf("  %s = %s\n", k, v)
	}

	fmt.Println("all keys, sorted, via explicit Iterator:")
	it := db.Iterator(kv.IterOptions{Sorted: true})
	defer it.Close()
	for it.Next() {
		fmt.Printf("  %s = %s\n", it.Key(), it.Value())
	}
	must(it.Err())

	// Typed access: values (de)serialized through a codec, keys namespaced
	// under the bucket prefix and returned prefix-stripped.
	users := kv.NewBucket[user](db, "users:", kv.JSONCodec[user]{})
	must(users.Put("carol", user{Name: "Carol", Age: 35}))
	must(users.Put("dave", user{Name: "Dave", Age: 41}))
	fmt.Println("typed bucket entries:")
	for name, u := range users.All("") {
		fmt.Printf("  %s: %+v\n", name, u)
	}

	must(db.Delete([]byte("user:2")))
	if _, err := db.Get([]byte("user:2")); errors.Is(err, kv.ErrNotFound) {
		fmt.Println("user:2 deleted as expected")
	}

	must(db.Sync())    // make everything written so far durable
	must(db.Compact()) // reclaim dead bytes now rather than on the ticker
	s := db.Stats()
	fmt.Printf("stats: keys=%d segments=%d deadBytes=%d\n", s.Keys, s.Segments, s.DeadBytes)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
