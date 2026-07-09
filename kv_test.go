package kv

import "testing"

func TestIteratorSorted(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keys := []string{"banana", "apple", "cherry", "date", "apricot"}
	for _, k := range keys {
		if err := db.Put([]byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	it := db.Iterator(IterOptions{Sorted: true})
	defer it.Close()
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"apple", "apricot", "banana", "cherry", "date"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestIteratorSortedReverse(t *testing.T) {
	db, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, k := range []string{"b", "a", "c"} {
		db.Put([]byte(k), []byte("v"))
	}

	it := db.Iterator(IterOptions{Sorted: true, Reverse: true})
	defer it.Close()
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"c", "b", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
