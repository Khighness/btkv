package btkv

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// @Author KHighness
// @Update 2022-11-14

func Test_Open(t *testing.T) {
	db, err := Open("tmp")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestDB_Set(t *testing.T) {
	db, err := Open("tmp")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix, valPrefix := "test_key_", "test_val_"
	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		if err = db.Set(key, val); err != nil {
			t.Error(err)
		}
	}
}

func TestDB_Get(t *testing.T) {
	db, err := Open("tmp")
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		}
		t.Logf("key = %s, val = %s\n", string(key), string(val))
	}

	for i := 0; i < 5; i++ {
		getVal([]byte("test_key_" + strconv.Itoa(i)))
	}
}

func TestDB_Del(t *testing.T) {
	db, err := Open("tmp")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_0")
	if err = db.Del(key); err != nil {
		t.Error("del err: ", err)
	}
}

func TestDB_Merge(t *testing.T) {
	db, err := Open("tmp")
	if err != nil {
		t.Error(err)
	}

	if err = db.Merge(); err != nil {
		t.Error("merge err: ", err)
	}
}
