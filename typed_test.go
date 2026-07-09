package kv

import "testing"

type user struct {
	Name string
	Age  int
}

func TestBucketTypedRoundtrip(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users := NewBucket[user](db, "users:", JSONCodec[user]{})
	if err := users.Put("alice", user{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	got, err := users.Get("alice")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Alice" || got.Age != 30 {
		t.Fatalf("got %+v", got)
	}

	has, err := users.Has("alice")
	if err != nil || !has {
		t.Fatalf("expected Has=true, err=%v", err)
	}
	if err := users.Delete("alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Get("alice"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestBucketNamespaceIsolation(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	a := NewBucket[user](db, "a:", JSONCodec[user]{})
	b := NewBucket[user](db, "b:", JSONCodec[user]{})
	a.Put("k", user{Name: "in-a"})
	b.Put("k", user{Name: "in-b"})

	gotA, err := a.Get("k")
	if err != nil || gotA.Name != "in-a" {
		t.Fatalf("bucket a: got %+v err=%v", gotA, err)
	}
	gotB, err := b.Get("k")
	if err != nil || gotB.Name != "in-b" {
		t.Fatalf("bucket b: got %+v err=%v", gotB, err)
	}
}
