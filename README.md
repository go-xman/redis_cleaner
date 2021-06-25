按前缀清理redis key
```shell
NAME:
   main scan - scan

USAGE:
   main scan [command options] [arguments...]

OPTIONS:
   --host value                      Redis host with port
   --passwd value, -p value          Redis password
   --db value                        Redis db num (default: 0)
   --key_match_rule value, -k value  Key matching rules, eg: aaa*
   --check_regex value, -c value     Use regular to check whether the key is valid
   --key_type value, -t value        指定要删除的key类型，类型支持string list set zset hash stream (默认不限)
   --batch_size value, -s value      The number of keys in batch deletion (default: 100)
   --qps value, -q value             Delete qps (default: 10)
   --exec                            Whether to delete, only the scanned key is checked by default (default: false)
   --help, -h                        show help (default: false)

```
