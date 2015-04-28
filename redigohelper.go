package redigohelper

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/northbright/fnlog"
	"log"
	"time"
)

var (
	logger           *log.Logger
	DEF_MAX_IDLE     int           = 3
	DEF_MAX_ACTIVE   int           = 1000
	DEF_IDLE_TIMEOUT time.Duration = 3 * time.Second
)

func CheckKey(key string) error {
	msg := ""
	if key == "" {
		msg = "empty key"
		logger.Println(msg)
		return errors.New(msg)
	}

	return nil
}

func CheckMap(m map[string]string) error {
	msg := ""
	if len(m) == 0 {
		msg = "empty map"
		logger.Println(msg)
		return errors.New(msg)
	}

	return nil
}

func HMSET(conn redis.Conn, key string, m map[string]string) error {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err"
		logger.Printf(msg)
		return err
	}

	if err := CheckMap(m); err != nil {
		msg = "CheckMap() err"
		logger.Printf(msg)
		return err
	}

	cmd := "HMSET"
	args := []interface{}{}
	args = append(args, key)
	for k, v := range m {
		args = append(args, k, v)
	}

	if _, err := conn.Do(cmd, args...); err != nil {
		msg = fmt.Sprintf("SetHash(): conn.Do(%v, %v): err: %v", cmd, args, err)
		logger.Printf(msg)
		return err
	}

	return nil
}

func HGETALL(conn redis.Conn, key string) (m map[string]string, err error) {
	if err = CheckKey(key); err != nil {
		return nil, err
	}

	cmd := "HGETALL"
	args := []interface{}{}
	args = append(args, key)

	if m, err = redis.StringMap(conn.Do(cmd, args...)); err != nil {
		logger.Printf("HGETALL(%v) error", key)
		return nil, err
	}

	return m, nil
}

// New a connection pool
// Params:
//     server: Redis Server Address. Ex: "192.168.0.1:8080", ":8080"
//     password: Redis Password
//     maxIdle: Maximum number of idle connections in the pool.
//         You may use DEF_MAX_IDLE: 3.
//     maxActive: Maximum number of connections allocated by the pool at a given time.
//         When zero, there is no limit on the number of connections in the pool.
//         You may use DEF_MAX_ACTIVE: 1000.
//     idleTimeoutSec: Close connections after remaining idle for this duration.
//         If the value is zero, then idle connections are not closed.
//         Applications should set the timeout to a value less than the server's timeout.
//         You may use DEF_IDLE_TIMEOUT: 3 * time.Second
// Return:
//     *redis.Pool
// References:
//     <http://godoc.org/github.com/garyburd/redigo/redis#Pool>
func NewPool(server, password string, maxIdle, maxActive int, idleTimeout time.Duration) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,     // default: 3
		MaxActive:   maxActive,   // default: 1000
		IdleTimeout: idleTimeout, // default 3 * 60 seconds
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func init() {
	logger = fnlog.New("")
}
