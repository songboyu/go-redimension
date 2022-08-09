package redimension

import (
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var c redis.Conn
var r *Redimension

func init() {
	var err error
	c, err = redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	r = NewRedimension(c, 2, 64)
}

func Test_encode(t *testing.T) {
	v := []uint32{0, 99328}
	assert.Equal(t, "000004facc99de3137e93b98a3100000", r.encode(v))
}

func Test_elestring(t *testing.T) {
	v := []uint32{0, 99328}
	s, err := r.elestring(v, "test")
	if err != nil {
		t.Errorf("elestring err: %v", err)
		return
	}
	assert.Equal(t, s, "000004facc99de3137e93b98a3100000:0:99328:test")
}

func Test_Query(t *testing.T) {
	zkey, hkey := "test_zkey", "test_hkey"
	r.Index(zkey, hkey, "aa", []uint32{45, 120000})
	r.Index(zkey, hkey, "bb", []uint32{50, 110000})
	r.Index(zkey, hkey, "cc", []uint32{30, 125000})
	vrange := make([][]uint32, 0)
	vrange = append(vrange, []uint32{40, 50})
	vrange = append(vrange, []uint32{100000, 115000})
	res, err := r.Query(zkey, vrange)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}
