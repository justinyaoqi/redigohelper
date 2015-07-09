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

func CheckKey(key string) error {
	msg := ""
	if key == "" {
		msg = "empty key\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return errors.New(msg)
	}

	return nil
}

func CheckMap(m map[string]string) error {
	msg := ""
	if len(m) == 0 {
		msg = "empty map\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return errors.New(msg)
	}

	return nil
}

func SET(conn redis.Conn, key, value string) error {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return err
	}

	cmd := "SET"
	args := []interface{}{}
	args = append(args, key, value)

	if _, err := conn.Do(cmd, args...); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return err
	}

	return nil
}

func GET(conn redis.Conn, key string) (value string, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return "", err
	}

	cmd := "GET"
	args := []interface{}{}
	args = append(args, key)

	if value, err = redis.String(conn.Do(cmd, args...)); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return "", err
	}

	return value, nil
}

func INCR(conn redis.Conn, key string) (n int64, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return 0, err
	}

	cmd := "INCR"
	args := []interface{}{}
	args = append(args, key)

	if n, err = redis.Int64(conn.Do(cmd, args...)); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return 0, err
	}

	return n, nil
}

func EXISTS(conn redis.Conn, key string) (exists bool, err error) {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return false, err
	}

	cmd := "EXISTS"
	args := []interface{}{}
	args = append(args, key)

	if exists, err = redis.Bool(conn.Do(cmd, args...)); err != nil {
		msg = fmt.Sprintf("conn.Do(%v, %v): err: %v\n", cmd, args, err)
		if DEBUG {
			fmt.Printf(msg)
		}
		return false, err
	}

	return exists, nil
}

func DEL(conn redis.Conn, keys []string) (n int64, err error) {
	msg := ""
	if len(keys) == 0 {
		return 0, errors.New("no keys")
	}

	cmd := "DEL"
	args := []interface{}{}

	for _, k := range keys {
		if err := CheckKey(k); err != nil {
			msg = "CheckKey() err\n"
			if DEBUG {
				fmt.Printf(msg)
			}
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

func HMSET(conn redis.Conn, key string, m map[string]string) error {
	msg := ""
	if err := CheckKey(key); err != nil {
		msg = "CheckKey() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
		return err
	}

	if err := CheckMap(m); err != nil {
		msg = "CheckMap() err\n"
		if DEBUG {
			fmt.Printf(msg)
		}
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

func HGETALL(conn redis.Conn, key string) (m map[string]string, err error) {
	if err = CheckKey(key); err != nil {
		return nil, err
	}

	cmd := "HGETALL"
	args := []interface{}{}
	args = append(args, key)

	if m, err = redis.StringMap(conn.Do(cmd, args...)); err != nil {
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
