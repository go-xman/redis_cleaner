package main

import (
	"log"
	"os"

	"github.com/go-redis/redis"
	"github.com/urfave/cli/v2"
	clear "redis_clear"
)

func main() {
	app := cli.NewApp()
	app.Commands = []*cli.Command{
		{
			Name:        "scan",
			Usage:       "scan",
			UsageText:   "",
			Description: "",
			Action: func(c *cli.Context) error {
				redisHost := c.String("host")
				redisPass := c.String("passwd")
				redisDB := c.Int("db")
				redisKey := c.String("key_match_rule")
				checkRegStr := c.String("check_regex")
				redisKeyType := c.String("key_type")
				deleteBatchSize := c.Int("batch_size")
				qps := c.Int("qps")
				exec := c.Bool("exec")

				rdb := redis.NewClient(&redis.Options{
					Addr:         redisHost,
					Password:     redisPass,
					DB:           redisDB,
					PoolSize:     10,
					MinIdleConns: 2,
				})
				defer rdb.Close()
				_, err := rdb.Ping().Result()
				if err != nil {
					return err
				}

				cleaner, err := clear.NewCleaner(redisKey, redisKeyType, checkRegStr, rdb, deleteBatchSize, qps, !exec)
				if err != nil {
					return err
				}

				log.Printf("开始执行，key:%s, key_type:%s, delete_batch_size:%d, qps:%d\n",
					redisKey,
					redisKeyType,
					deleteBatchSize,
					qps,
				)
				cleaner.Run()
				log.Printf("执行结束")
				return nil
			},
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "host",
					Usage:    "Redis host with port",
					Required: true,
				},
				&cli.StringFlag{
					Name:    "passwd",
					Aliases: []string{"p"},
					Usage:   "Redis password",
				},
				&cli.IntFlag{
					Name:  "db",
					Usage: "Redis db num",
					Value: 0,
				},
				&cli.StringFlag{
					Name:     "key_match_rule",
					Aliases:  []string{"k"},
					Usage:    "Key matching rules, eg: aaa*",
					Value:    "",
					Required: true,
				},
				&cli.StringFlag{
					Name:    "check_regex",
					Aliases: []string{"c"},
					Usage:   "Use regular to check whether the key is valid",
					Value:   "",
				},
				&cli.StringFlag{
					Name:    "key_type",
					Aliases: []string{"t"},
					Usage:   "指定要删除的key类型，类型支持string list set zset hash stream (默认不限)",
					Value:   "",
				},
				&cli.IntFlag{
					Name:    "batch_size",
					Aliases: []string{"s"},
					Usage:   "The number of keys in batch deletion",
					Value:   100,
				},
				&cli.IntFlag{
					Name:    "qps",
					Aliases: []string{"q"},
					Usage:   "Delete qps",
					Value:   10,
				},
				&cli.BoolFlag{
					Name:  "exec",
					Usage: "Whether to delete, only the scanned key is checked by default",
					Value: false,
				},
			},
		},
	}
	_ = app.Run(os.Args)
}
