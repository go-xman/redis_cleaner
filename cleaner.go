package clear

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"go.uber.org/ratelimit"
)

var (
	validKeys = []string{"string", "list", "set", "zset", "hash", "stream"}
)

const (
	delScript = `if redis.call("TYPE", KEYS[1]).ok == KEYS[2] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`
)

func NewCleaner(redisKey, redisKeyType, checkRegStr string, redis *redis.Client, deleteBatchSize, qps int, testMode bool) (*Cleaner, error) {
	if redisKeyType != "" && !InStrings(redisKeyType, validKeys...) {
		return nil, errors.New("invalid redis key type")
	}

	c := &Cleaner{
		KeyMatch:         redisKey,
		KeyType:          strings.ToLower(redisKeyType),
		Rdb:              redis,
		DeleteBatchCount: deleteBatchSize,
		TestMode:         testMode,
	}
	if qps == 0 {
		qps = 10
	}
	if checkRegStr != "" {
		c.check = regexp.MustCompile(checkRegStr)
	}
	c.Limit = ratelimit.New(qps)
	return c, nil
}

type Cleaner struct {
	KeyMatch         string
	KeyType          string
	Rdb              *redis.Client
	DeleteBatchCount int
	TestMode         bool
	check            *regexp.Regexp
	Limit            ratelimit.Limiter
}

func (p *Cleaner) Run() {
	group := NewGroup(10, func(i interface{}) {
		p.DelKeys(i.([]string))
	})

	keyChan := p.ScanKeys()

	var keyBuf []string
	var num int64
	for key := range keyChan {
		keyBuf = append(keyBuf, key)
		if len(keyBuf) == p.DeleteBatchCount {
			tmp := make([]string, len(keyBuf))
			copy(tmp, keyBuf)
			group.Add(strconv.FormatInt(num, 10), tmp)
			keyBuf = keyBuf[:0]
		}
	}

	if len(keyBuf) > 0 {
		group.Add(strconv.FormatInt(num, 10), keyBuf)
	}
	group.Sync()
}

func (p *Cleaner) DelKeys(keys []string) {
	log.Println("key列表:", keys)
	log.Println("p.KeyType:", p.KeyType)
	if !p.TestMode {
		if p.KeyType == "" {
			p.Limit.Take()
			p.Rdb.Del(keys...)
		} else {
			for _, key := range keys {
				p.Limit.Take()
				_, err := p.Rdb.Eval(delScript, []string{key, p.KeyType}).Result()
				if err != nil {
					log.Println("删除出错", err)
				}
			}
		}
	}
}

func (p *Cleaner) ScanKeys() <-chan string {
	ch := make(chan string, 100)

	go func() {
		defer close(ch)

		var (
			cursor uint64
			keys   []string
			err    error
			count  = int64(p.DeleteBatchCount)
		)
		for {
			keys, cursor, err = p.Rdb.Scan(cursor, p.KeyMatch, count).Result()
			if err != nil {
				log.Println("scan error, err:", err)
				break
			}
			for _, key := range keys {
				if p.check != nil && p.check.MatchString(key) {
					ch <- key
				}
			}

			if cursor == 0 {
				// end
				break
			}

			if cursor > 0 && len(keys) == 0 {
				count += count/10 + 1
				log.Println("扩大扫描数量，count:", count)
			} else if count > int64(p.DeleteBatchCount) {
				count -= int64(p.DeleteBatchCount)/10 - 1
			}
		}
	}()

	return ch
}
