package redigohelper

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
)

var (
	pool          *redis.Pool
	redisServer   = ":6379"
	redisPassword = "123456"
)

func TestRediaoHelper(t *testing.T) {
	logger.Printf("NewPool()...")

	pool := NewPool(redisServer, redisPassword, DEF_MAX_IDLE, DEF_MAX_ACTIVE, DEF_IDLE_TIMEOUT)

	conn := pool.Get()
	defer conn.Close()

	m := map[string]string{}
	m["name"] = "王老师"
	m["mobile"] = "13800138000"
	m["sex"] = "male"
	m["birthyear"] = "1980"

	k := "teacher:1"

	if err := HMSET(conn, k, m); err != nil {
		msg := fmt.Sprintf("HMSET(%v, %v), err: %v", k, m, err)
		t.Error(msg)
	} else {
		logger.Printf("HSET(conn, %v, m): ok", k)
	}

	m, err := HGETALL(conn, k)
	if err != nil {
		msg := fmt.Sprintf("HGETALL(conn, %v), err: %v", k, err)
		t.Error(msg)
	} else {
		logger.Printf("HGETALL(%v): %v", k, m)
	}

	k = "mykey"
	v := "myvalue"
	if err = SET(conn, k, v); err != nil {
		msg := fmt.Sprintf("SET(conn, %v, %v), err: %v", k, v, err)
		t.Error(msg)
	} else {
		logger.Printf("SET(conn, %v, %v): OK", k, v)
	}

	if v, err = GET(conn, k); err != nil {
		msg := fmt.Sprintf("GET(conn, %v), err: %v", k, err)
		t.Error(msg)
	} else {
		logger.Printf("GET(conn, %v): %v", k, v)
	}

	k = "lastid"
	for i := 0; i < 5; i++ {
		if n, err := INCR(conn, k); err != nil {
			msg := fmt.Sprintf("INCR(conn, %v), err: %v", k, err)
			t.Error(msg)
		} else {
			logger.Printf("INCR(conn, %v): %v", k, n)
		}
	}

}
