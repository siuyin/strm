package strm

import (
	"testing"

	"github.com/nats-io/nuid"
)

func TestDBFunctions(t *testing.T) {
	bucketName := nuid.Next()
	db := DBInit(bucketName)

	t.Run("put and get", func(t *testing.T) {
		if _, err := db.kv.PutString("a", "apple"); err != nil {
			t.Error(err)
		}
		if v, err := db.kv.Get("a"); string(v.Value()) != "apple" || err != nil {
			t.Errorf("unable to get key 'a':  %v", err)
		}
		if err := db.kv.Delete("a"); err != nil {
			t.Error(err)
		}
	})

	if err := s.js.DeleteKeyValue(bucketName); err != nil {
		t.Error(err)
	}
	db.Close()
}
