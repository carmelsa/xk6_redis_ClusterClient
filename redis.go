package rediscluster

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
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
func (r *Redis) XClient(ctxPtr *context.Context, server string, max int, timeout int) interface{} {
	servers := []string{server}
	opts := &redis.ClusterOptions{Addrs: servers, PoolTimeout: time.Duration(timeout), PoolSize: max}
	rt := common.GetRuntime(*ctxPtr)
	ClusterClient := Clusterclient{clusterclient: redis.NewClusterClient(opts)}
	return common.Bind(rt, &ClusterClient, ctxPtr)
}

func (c *Clusterclient) Flushall() {
	_, err := c.clusterclient.FlushAll().Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error flush all data %v", err))
	}
}

// Set the given key with the given value and expiration time.
func (c *Clusterclient) Set(key, value string, exp time.Duration) {
	_, err := c.clusterclient.Set(key, value, exp).Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("error seting key %v", err))
	}
}

// Get returns the value for the given key.
func (c *Clusterclient) Get(key string) (string, error) {
	res, err := c.clusterclient.Get(key).Result()
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
	res, err := c.clusterclient.ClusterInfo().Result()
	if err != nil {
		return "", err
	}
	return res, nil
}
