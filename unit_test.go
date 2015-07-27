package redigohelper_test

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/northbright/redigohelper"
	"os"
)

var (
	pool          *redis.Pool
	redisServer   = ":6379"
	redisPassword = "123456"
)

func Example() {
	fmt.Fprintf(os.Stderr, "NewPool()...\n")

	// Create Redis connection pool
	//pool := redigohelper.NewPool(redisServer, redisPassword, redigohelper.DEF_MAX_IDLE, redigohelper.DEF_MAX_ACTIVE, redigohelper.DEF_IDLE_TIMEOUT)

	// Create Redis connection pool with default arguments.
	pool := redigohelper.NewDefaultPool(redisServer, redisPassword)

	conn := pool.Get()
	defer conn.Close()

	m := map[string]string{}
	m["name"] = "王老师"
	m["mobile"] = "13800138000"
	m["sex"] = "male"
	m["birthyear"] = "1980"

	k := "teacher:1"

	// HMSET
	if err := redigohelper.HMSET(conn, k, m); err != nil {
		msg := fmt.Sprintf("HMSET(%v, %v), err: %v\n", k, m, err)
		fmt.Println(msg)
	} else {
		fmt.Fprintf(os.Stderr, "HSET(conn, %v, m): ok\n", k)
	}

	// HGETALL
	m, err := redigohelper.HGETALL(conn, k)
	if err != nil {
		msg := fmt.Sprintf("HGETALL(conn, %v), err: %v\n", k, err)
		fmt.Println(msg)
	} else {
		fmt.Fprintf(os.Stderr, "HGETALL(%v): %v\n", k, m)
	}

	// INCR
	k = "lastid"
	for i := 0; i < 5; i++ {
		if n, err := redigohelper.INCR(conn, k); err != nil {
			msg := fmt.Sprintf("INCR(conn, %v), err: %v\n", k, err)
			fmt.Println(msg)
		} else {
			fmt.Fprintf(os.Stderr, "INCR(conn, %v): %v\n", k, n)
		}
	}

	// EXISTS
	keyArr := []string{"not_exist_key", "lastid"}

	for _, k := range keyArr {
		if b, err := redigohelper.EXISTS(conn, k); err != nil {
			msg := fmt.Sprintf("EXISTS(conn, %v), err: %v\n", k, err)
			fmt.Println(msg)
		} else {
			fmt.Fprintf(os.Stderr, "EXISTS(conn, %v): %v\n", k, b)
		}
	}

	// GET / SET
	k = "not_exist_key"
	if v, err := redigohelper.GET(conn, k); err != nil {
		msg := fmt.Sprintf("GET(conn, %v), err: %v\n", k, err)
		fmt.Fprintf(os.Stderr, msg)
	} else {
		fmt.Fprintf(os.Stderr, "GET(conn, %v): %v\n", k, v)
	}

	v := "myvalue"
	if err = redigohelper.SET(conn, k, v); err != nil {
		msg := fmt.Sprintf("SET(conn, %v, %v), err: %v\n", k, v, err)
		fmt.Println(msg)
	} else {
		fmt.Fprintf(os.Stderr, "SET(conn, %v, %v): OK\n", k, v)
	}

	if v, err = redigohelper.GET(conn, k); err != nil {
		msg := fmt.Sprintf("GET(conn, %v), err: %v\n", k, err)
		fmt.Println(msg)
	} else {
		fmt.Fprintf(os.Stderr, "GET(conn, %v): %v\n", k, v)
	}

	// DEL
	keyArr = []string{"not_exist_key"}
	if n, err := redigohelper.DEL(conn, keyArr); err != nil {
		msg := fmt.Sprintf("DEL(conn, %v), err: %v\n", keyArr, err)
		fmt.Println(msg)
	} else {
		fmt.Fprintf(os.Stderr, "DEL(conn, %v): %v\n", keyArr, n)
	}

	// Output:
}
