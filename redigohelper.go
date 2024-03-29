// Package redigohelper provides helper functions to make it easy to use redigo - http://github.com/garyburd/redigo.
package redigohelper

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	DEF_MAX_IDLE     int           = 3                 // Default maximum number of idle connections in the pool.
	DEF_MAX_ACTIVE   int           = 1000              // Default maximum number of connections allocated by the pool at a given time. When zero, there's no limit on the number of connections.
	DEF_IDLE_TIMEOUT time.Duration = 180 * time.Second // Default duration that it'll close connections after remaining idle for this duration.
)

var (
	DEBUG bool = false // Set it to true to output debug messages from this package.
)

// CheckKey() Checks if key is empty.
func CheckKey(key string) error {
	msg := ""
	if key == "" {
		msg = "empty key."
		if DEBUG {
			fmt.Println(msg)
		}
		return errors.New(msg)
	}

	return nil
}

// CheckMap() Checks if map is empty.
func CheckMap(m map[string]string) error {
	msg := ""
	if len(m) == 0 {
		msg = "empty map."
		if DEBUG {
			fmt.Println(msg)
		}
		return errors.New(msg)
	}

	return nil
}

// SET() does the "SET" command.
func SET(conn redis.Conn, key, value string) error {
	msg := ""
	if err := CheckKey(key); err != nil {
		return err
	}

	if _, err := conn.Do("SET", key, value); err != nil {
		msg = fmt.Sprintf("conn.Do(SET, %v, %v): err: %v\n", key, value, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return err
	}

	return nil
}

// GET() does the "GET" command.
func GET(conn redis.Conn, key string) (value string, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		return "", err
	}

	if value, err = redis.String(conn.Do("GET", key)); err != nil {
		msg = fmt.Sprintf("conn.Do(GET, %v): err: %v\n", key, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return "", err
	}

	return value, nil
}

// INCR() does the "INCR" command.
func INCR(conn redis.Conn, key string) (n int64, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		return 0, err
	}

	if n, err = redis.Int64(conn.Do("INCR", key)); err != nil {
		msg = fmt.Sprintf("conn.Do(INCR, %v): err: %v\n", key, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return 0, err
	}

	return n, nil
}

// EXISTS() does the "EXISTS" command.
func EXISTS(conn redis.Conn, key string) (exists bool, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		return false, err
	}

	if exists, err = redis.Bool(conn.Do("EXISTS", key)); err != nil {
		msg = fmt.Sprintf("conn.Do(EXISTS, %v): err: %v\n", key, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return false, err
	}

	return exists, nil
}

// DEL() does the "DEL" command.
//
//   Params:
//       conn: redis.Conn
//       keys: the keys to be deleted.
//   Return:
//       n: The number of keys that were removed.
//       err: nil if no error occurs or specified error otherwise.
func DEL(conn redis.Conn, keys []string) (n int64, err error) {
	msg := ""
	if len(keys) == 0 {
		return 0, errors.New("no keys")
	}

	cmd := "DEL"
	args := []interface{}{}

	for _, k := range keys {
		if err := CheckKey(k); err != nil {
			return 0, err
		} else {
			args = append(args, k)
		}
	}

	if n, err = redis.Int64(conn.Do(cmd, args...)); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return 0, err
	}

	return n, nil
}

// HMSET() does the "HMSET" command.
//
//   Params:
//       conn: redis.Conn.
//       key: key to store the hash.
//       m: map contains the specified fields and their respective values.
//   Return:
//       nil if no error occurs or specified error otherwise.
func HMSET(conn redis.Conn, key string, m map[string]string) error {
	msg := ""
	if err := CheckKey(key); err != nil {
		return err
	}

	if err := CheckMap(m); err != nil {
		return err
	}

	cmd := "HMSET"
	args := []interface{}{}
	args = append(args, key)
	for k, v := range m {
		args = append(args, k, v)
	}

	if _, err := conn.Do(cmd, args...); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return err
	}

	return nil
}

// HGETALL() does the "HGETALL" command.
//
//   Params:
//       conn: redis.Conn.
//       key: key stores the hash.
//   Return:
//       m: map contains the specified fields and their respective values.
//       err: nil if no error occurs or specified error otherwise.
func HGETALL(conn redis.Conn, key string) (m map[string]string, err error) {
	if err = CheckKey(key); err != nil {
		return nil, err
	}

	if m, err = redis.StringMap(conn.Do("HGETALL", key)); err != nil {
		if DEBUG {
			fmt.Printf("HGETALL(%v) err: %v\n", key, err)
		}
		return nil, err
	}

	return m, nil
}

// NewPool() creates a connection pool.
//
//   Params:
//       server: Redis Server Address. Ex: "192.168.0.1:8080", ":8080"
//       password: Redis Password
//       maxIdle: Maximum number of idle connections in the pool.
//           You may use DEF_MAX_IDLE: 3.
//       maxActive: Maximum number of connections allocated by the pool at a given time.
//           When zero, there is no limit on the number of connections in the pool.
//           You may use DEF_MAX_ACTIVE: 1000.
//       idleTimeoutSec: Close connections after remaining idle for this duration.
//           If the value is zero, then idle connections are not closed.
//           Applications should set the timeout to a value less than the server's timeout.
//           You may use DEF_IDLE_TIMEOUT: 180 * time.Second
//   Return:
//       *redis.Pool
//   References:
//       <http://godoc.org/github.com/garyburd/redigo/redis#Pool>
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

// NewDefaultPool() creates a connection pool with default parameters.
func NewDefaultPool(server, password string) *redis.Pool {
	return NewPool(server, password, DEF_MAX_IDLE, DEF_MAX_ACTIVE, DEF_IDLE_TIMEOUT)
}
