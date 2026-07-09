// Command example is a minimal, runnable demonstration of the kv package:
// open a DB, Put/Get/Delete a few keys, iterate by prefix, and close.
package main

import (
	"fmt"
	"log"
	"os"

	kv "github.com/Gandalfalex/in_memory_db"
)

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

	fmt.Println("keys with prefix \"user:\":")
	it := db.Iterator(kv.IterOptions{Prefix: []byte("user:")})
	defer it.Close()
	for it.Next() {
		fmt.Printf("  %s = %s\n", it.Key(), it.Value())
	}

	must(db.Delete([]byte("user:2")))
	if _, err := db.Get([]byte("user:2")); err == kv.ErrNotFound {
		fmt.Println("user:2 deleted as expected")
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
