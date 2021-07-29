package rediscluster

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"time"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/redis".
func init() {
	modules.Register("k6/x/rediscluster", new(Redis))
}

// Redis is the k6 extension for a Redis client.
type Redis struct{}

// Client is the Redis client wrapper.
type Client struct {
	client *redis.Client
}

type Clusterclient struct {
	clusterclient *redis.ClusterClient
}

// XClient represents the Cluster client constructor (i.e. `new redis.ClusterClient()`) and
// returns a new Redis Cluster client object.
func (r *Redis) XClient(ctxPtr *context.Context, server string, max int, timeout int, maxRetries int) interface{} {
	servers := []string{server}
	opts := &redis.ClusterOptions{Addrs: servers, PoolTimeout: time.Duration(timeout), PoolSize: max, MaxRetries: maxRetries}
	rt := common.GetRuntime(*ctxPtr)
	ClusterClient := Clusterclient{clusterclient: redis.NewClusterClient(opts)}
	return common.Bind(rt, &ClusterClient, ctxPtr)
}

func (c *Clusterclient) Flushall() {
	_, err := c.clusterclient.FlushAll(c.clusterclient.Context()).Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error flush all data %v", err))
	}
}

// Set the given key with the given value and expiration time.
func (c *Clusterclient) Set(key, value string, exp time.Duration) {
	_, err := c.clusterclient.Set(c.clusterclient.Context(), key, value, exp).Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error seting key %v", err))
	}
}

// Get returns the value for the given key.
func (c *Clusterclient) Get(key string) (string, error) {
	res, err := c.clusterclient.Get(c.clusterclient.Context(), key).Result()
	if err != nil {
		return "", err
	}
	return res, nil
}

// Close returns the value for the given key.
func (c *Clusterclient) Close() {
	err := c.clusterclient.Close()
	if err != nil {
		fmt.Println(fmt.Sprintf("error close connection %v", err))
	}
}

// Clientinfo returns the Client info.
func (c *Clusterclient) Clientinfo() (string, error) {
	res, err := c.clusterclient.ClusterInfo(c.clusterclient.Context()).Result()
	if err != nil {
		return "", err
	}
	return res, nil
}

// Setdo the given key with the given value and expiration time.
func (c *Clusterclient) Setdo(key, value string, exp time.Duration) {
	_, err := c.clusterclient.Do(c.clusterclient.Context(), "SET", key, value, exp).Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error seting key %v", err))
	}
}

// Getdo returns the value for the given key.
func (c *Clusterclient) Getdo(key string) (string, error) {
	res, err := c.clusterclient.Do(c.clusterclient.Context(), "GET", key).Result()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", res), nil
}

// Setnx the given key with the given value and expiration time.
func (c *Clusterclient) Setnx(key, value string, exp time.Duration) {
	_, err := c.clusterclient.SetNX(c.clusterclient.Context(), key, value, exp).Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error seting key %v", err))
	}
}
