package redis

import (
	"context"
	goredis "github.com/go-redis/redis/v8"
	"github.com/goal-web/contracts"
	"time"
)

type Connection struct {
	exceptionHandler contracts.ExceptionHandler
	client           *goredis.Client
}

func (conn *Connection) Subscribe(channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := conn.client.Subscribe(context.Background(), channels...)
	if pingErr := pubSub.Ping(context.Background(), ""); pingErr != nil {
		return pingErr
	}

	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				conn.exceptionHandler.Handle(&SubscribeException{Err: err})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (conn *Connection) PSubscribe(channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := conn.client.PSubscribe(context.Background(), channels...)
	if pingErr := pubSub.Ping(context.Background(), ""); pingErr != nil {
		return pingErr
	}
	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				conn.exceptionHandler.Handle(&SubscribeException{Err: err})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (conn *Connection) Command(method string, args ...any) (any, error) {
	return conn.client.Do(context.Background(), append([]any{method}, args...)...).Result()
}

func (conn *Connection) PubSubChannels(pattern string) ([]string, error) {
	return conn.client.PubSubChannels(context.Background(), pattern).Result()
}

func (conn *Connection) PubSubNumSub(channels ...string) (map[string]int64, error) {
	return conn.client.PubSubNumSub(context.Background(), channels...).Result()
}

func (conn *Connection) PubSubNumPat() (int64, error) {
	return conn.client.PubSubNumPat(context.Background()).Result()
}

func (conn *Connection) Publish(channel string, message any) (int64, error) {
	return conn.client.Publish(context.Background(), channel, message).Result()
}

func (conn *Connection) Client() *goredis.Client {
	return conn.client
}

// getter start
func (conn *Connection) Get(key string) (string, error) {
	return conn.client.Get(context.Background(), key).Result()
}

func (conn *Connection) MGet(keys ...string) ([]any, error) {
	return conn.client.MGet(context.Background(), keys...).Result()
}

func (conn *Connection) GetBit(key string, offset int64) (int64, error) {
	return conn.client.GetBit(context.Background(), key, offset).Result()
}

func (conn *Connection) BitOpAnd(destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpAnd(context.Background(), destKey, keys...).Result()
}

func (conn *Connection) BitOpNot(destKey string, key string) (int64, error) {
	return conn.client.BitOpNot(context.Background(), destKey, key).Result()
}

func (conn *Connection) BitOpOr(destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpOr(context.Background(), destKey, keys...).Result()
}

func (conn *Connection) BitOpXor(destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpXor(context.Background(), destKey, keys...).Result()
}

func (conn *Connection) GetDel(key string) (string, error) {
	return conn.client.GetDel(context.Background(), key).Result()
}

func (conn *Connection) GetEx(key string, expiration time.Duration) (string, error) {
	return conn.client.GetEx(context.Background(), key, expiration).Result()
}

func (conn *Connection) GetRange(key string, start, end int64) (string, error) {
	return conn.client.GetRange(context.Background(), key, start, end).Result()
}

func (conn *Connection) GetSet(key string, value any) (string, error) {
	return conn.client.GetSet(context.Background(), key, value).Result()
}

func (conn *Connection) ClientGetName() (string, error) {
	return conn.client.ClientGetName(context.Background()).Result()
}

func (conn *Connection) StrLen(key string) (int64, error) {
	return conn.client.StrLen(context.Background(), key).Result()
}

// getter end
// keys start

func (conn *Connection) Keys(pattern string) ([]string, error) {
	return conn.client.Keys(context.Background(), pattern).Result()
}

func (conn *Connection) Del(keys ...string) (int64, error) {
	return conn.client.Del(context.Background(), keys...).Result()
}

func (conn *Connection) FlushAll() (string, error) {
	return conn.client.FlushAll(context.Background()).Result()
}

func (conn *Connection) FlushDB() (string, error) {
	return conn.client.FlushDB(context.Background()).Result()
}

func (conn *Connection) Dump(key string) (string, error) {
	return conn.client.Dump(context.Background(), key).Result()
}

func (conn *Connection) Exists(keys ...string) (int64, error) {
	return conn.client.Exists(context.Background(), keys...).Result()
}

func (conn *Connection) Expire(key string, expiration time.Duration) (bool, error) {
	return conn.client.Expire(context.Background(), key, expiration).Result()
}

func (conn *Connection) ExpireAt(key string, tm time.Time) (bool, error) {
	return conn.client.ExpireAt(context.Background(), key, tm).Result()
}

func (conn *Connection) PExpire(key string, expiration time.Duration) (bool, error) {
	return conn.client.PExpire(context.Background(), key, expiration).Result()
}

func (conn *Connection) PExpireAt(key string, tm time.Time) (bool, error) {
	return conn.client.PExpireAt(context.Background(), key, tm).Result()
}

func (conn *Connection) Migrate(host, port, key string, db int, timeout time.Duration) (string, error) {
	return conn.client.Migrate(context.Background(), host, port, key, db, timeout).Result()
}

func (conn *Connection) Move(key string, db int) (bool, error) {
	return conn.client.Move(context.Background(), key, db).Result()
}

func (conn *Connection) Persist(key string) (bool, error) {
	return conn.client.Persist(context.Background(), key).Result()
}

func (conn *Connection) PTTL(key string) (time.Duration, error) {
	return conn.client.PTTL(context.Background(), key).Result()
}

func (conn *Connection) TTL(key string) (time.Duration, error) {
	return conn.client.TTL(context.Background(), key).Result()
}

func (conn *Connection) RandomKey() (string, error) {
	return conn.client.RandomKey(context.Background()).Result()
}

func (conn *Connection) Rename(key, newKey string) (string, error) {
	return conn.client.Rename(context.Background(), key, newKey).Result()
}

func (conn *Connection) RenameNX(key, newKey string) (bool, error) {
	return conn.client.RenameNX(context.Background(), key, newKey).Result()
}

func (conn *Connection) Type(key string) (string, error) {
	return conn.client.Type(context.Background(), key).Result()
}

func (conn *Connection) Wait(numSlaves int, timeout time.Duration) (int64, error) {
	return conn.client.Wait(context.Background(), numSlaves, timeout).Result()
}

func (conn *Connection) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.Scan(context.Background(), cursor, match, count).Result()
}

func (conn *Connection) BitCount(key string, count *contracts.BitCount) (int64, error) {
	return conn.client.BitCount(context.Background(), key, &goredis.BitCount{
		Start: count.Start,
		End:   count.End,
	}).Result()
}

// keys end

// setter start
func (conn *Connection) Set(key string, value any, expiration time.Duration) (string, error) {
	return conn.client.Set(context.Background(), key, value, expiration).Result()
}

func (conn *Connection) Append(key, value string) (int64, error) {
	return conn.client.Append(context.Background(), key, value).Result()
}

func (conn *Connection) MSet(values ...any) (string, error) {
	return conn.client.MSet(context.Background(), values...).Result()
}

func (conn *Connection) MSetNX(values ...any) (bool, error) {
	return conn.client.MSetNX(context.Background(), values...).Result()
}

func (conn *Connection) SetNX(key string, value any, expiration time.Duration) (bool, error) {
	return conn.client.SetNX(context.Background(), key, value, expiration).Result()
}

func (conn *Connection) SetEX(key string, value any, expiration time.Duration) (string, error) {
	return conn.client.SetEX(context.Background(), key, value, expiration).Result()
}

func (conn *Connection) SetBit(key string, offset int64, value int) (int64, error) {
	return conn.client.SetBit(context.Background(), key, offset, value).Result()
}

func (conn *Connection) BitPos(key string, bit int64, pos ...int64) (int64, error) {
	return conn.client.BitPos(context.Background(), key, bit, pos...).Result()
}

func (conn *Connection) SetRange(key string, offset int64, value string) (int64, error) {
	return conn.client.SetRange(context.Background(), key, offset, value).Result()
}

func (conn *Connection) Incr(key string) (int64, error) {
	return conn.client.Incr(context.Background(), key).Result()
}

func (conn *Connection) Decr(key string) (int64, error) {
	return conn.client.Decr(context.Background(), key).Result()
}

func (conn *Connection) IncrBy(key string, value int64) (int64, error) {
	return conn.client.IncrBy(context.Background(), key, value).Result()
}

func (conn *Connection) DecrBy(key string, value int64) (int64, error) {
	return conn.client.DecrBy(context.Background(), key, value).Result()
}

func (conn *Connection) IncrByFloat(key string, value float64) (float64, error) {
	return conn.client.IncrByFloat(context.Background(), key, value).Result()
}

// setter end

// hash start
func (conn *Connection) HGet(key, field string) (string, error) {
	return conn.client.HGet(context.Background(), key, field).Result()
}

func (conn *Connection) HGetAll(key string) (map[string]string, error) {
	return conn.client.HGetAll(context.Background(), key).Result()
}

func (conn *Connection) HMGet(key string, fields ...string) ([]any, error) {
	return conn.client.HMGet(context.Background(), key, fields...).Result()
}

func (conn *Connection) HKeys(key string) ([]string, error) {
	return conn.client.HKeys(context.Background(), key).Result()
}

func (conn *Connection) HLen(key string) (int64, error) {
	return conn.client.HLen(context.Background(), key).Result()
}

func (conn *Connection) HRandField(key string, count int, withValues bool) ([]string, error) {
	return conn.client.HRandField(context.Background(), key, count, withValues).Result()
}

func (conn *Connection) HScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.HScan(context.Background(), key, cursor, match, count).Result()
}

func (conn *Connection) HValues(key string) ([]string, error) {
	return conn.client.HVals(context.Background(), key).Result()
}

func (conn *Connection) HSet(key string, values ...any) (int64, error) {
	return conn.client.HSet(context.Background(), key, values...).Result()
}

func (conn *Connection) HSetNX(key, field string, value any) (bool, error) {
	return conn.client.HSetNX(context.Background(), key, field, value).Result()
}

func (conn *Connection) HMSet(key string, values ...any) (bool, error) {
	return conn.client.HMSet(context.Background(), key, values...).Result()
}

func (conn *Connection) HDel(key string, fields ...string) (int64, error) {
	return conn.client.HDel(context.Background(), key, fields...).Result()
}

func (conn *Connection) HExists(key string, field string) (bool, error) {
	return conn.client.HExists(context.Background(), key, field).Result()
}

func (conn *Connection) HIncrBy(key string, field string, value int64) (int64, error) {
	return conn.client.HIncrBy(context.Background(), key, field, value).Result()
}

func (conn *Connection) HIncrByFloat(key string, field string, value float64) (float64, error) {
	return conn.client.HIncrByFloat(context.Background(), key, field, value).Result()
}

// hash end

// set start
func (conn *Connection) SAdd(key string, members ...any) (int64, error) {
	return conn.client.SAdd(context.Background(), key, members...).Result()
}

func (conn *Connection) SCard(key string) (int64, error) {
	return conn.client.SCard(context.Background(), key).Result()
}

func (conn *Connection) SDiff(keys ...string) ([]string, error) {
	return conn.client.SDiff(context.Background(), keys...).Result()
}

func (conn *Connection) SDiffStore(destination string, keys ...string) (int64, error) {
	return conn.client.SDiffStore(context.Background(), destination, keys...).Result()
}

func (conn *Connection) SInter(keys ...string) ([]string, error) {
	return conn.client.SInter(context.Background(), keys...).Result()
}

func (conn *Connection) SInterStore(destination string, keys ...string) (int64, error) {
	return conn.client.SInterStore(context.Background(), destination, keys...).Result()
}

func (conn *Connection) SIsMember(key string, member any) (bool, error) {
	return conn.client.SIsMember(context.Background(), key, member).Result()
}

func (conn *Connection) SMembers(key string) ([]string, error) {
	return conn.client.SMembers(context.Background(), key).Result()
}

func (conn *Connection) SRem(key string, members ...any) (int64, error) {
	return conn.client.SRem(context.Background(), key, members...).Result()
}

func (conn *Connection) SPopN(key string, count int64) ([]string, error) {
	return conn.client.SPopN(context.Background(), key, count).Result()
}

func (conn *Connection) SPop(key string) (string, error) {
	return conn.client.SPop(context.Background(), key).Result()
}

func (conn *Connection) SRandMemberN(key string, count int64) ([]string, error) {
	return conn.client.SRandMemberN(context.Background(), key, count).Result()
}

func (conn *Connection) SMove(source, destination string, member any) (bool, error) {
	return conn.client.SMove(context.Background(), source, destination, member).Result()
}

func (conn *Connection) SRandMember(key string) (string, error) {
	return conn.client.SRandMember(context.Background(), key).Result()
}

func (conn *Connection) SUnion(keys ...string) ([]string, error) {
	return conn.client.SUnion(context.Background(), keys...).Result()
}

func (conn *Connection) SUnionStore(destination string, keys ...string) (int64, error) {
	return conn.client.SUnionStore(context.Background(), destination, keys...).Result()
}

// set end

// geo start

func (conn *Connection) GeoAdd(key string, geoLocation ...*contracts.GeoLocation) (int64, error) {
	goredisLocations := make([]*goredis.GeoLocation, 0)
	for locationKey, value := range geoLocation {
		goredisLocations[locationKey] = &goredis.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return conn.client.GeoAdd(context.Background(), key, goredisLocations...).Result()
}

func (conn *Connection) GeoHash(key string, members ...string) ([]string, error) {
	return conn.client.GeoHash(context.Background(), key, members...).Result()
}

func (conn *Connection) GeoPos(key string, members ...string) ([]*contracts.GeoPos, error) {
	results := make([]*contracts.GeoPos, 0)
	goredisResults, err := conn.client.GeoPos(context.Background(), key, members...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = &contracts.GeoPos{
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
		}
	}
	return results, err
}

func (conn *Connection) GeoDist(key string, member1, member2, unit string) (float64, error) {
	return conn.client.GeoDist(context.Background(), key, member1, member2, unit).Result()
}

func (conn *Connection) GeoRadius(key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := conn.client.GeoRadius(context.Background(), key, longitude, latitude, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return results, err
}

func (conn *Connection) GeoRadiusStore(key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) (int64, error) {
	return conn.client.GeoRadiusStore(context.Background(), key, longitude, latitude, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
}

func (conn *Connection) GeoRadiusByMember(key, member string, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := conn.client.GeoRadiusByMember(context.Background(), key, member, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return results, err
}

func (conn *Connection) GeoRadiusByMemberStore(key, member string, query *contracts.GeoRadiusQuery) (int64, error) {
	return conn.client.GeoRadiusByMemberStore(context.Background(), key, member, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
}

// geo end

// lists start

func (conn *Connection) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return conn.client.BLPop(context.Background(), timeout, keys...).Result()
}

func (conn *Connection) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	return conn.client.BRPop(context.Background(), timeout, keys...).Result()
}

func (conn *Connection) BRPopLPush(source, destination string, timeout time.Duration) (string, error) {
	return conn.client.BRPopLPush(context.Background(), source, destination, timeout).Result()
}

func (conn *Connection) LIndex(key string, index int64) (string, error) {
	return conn.client.LIndex(context.Background(), key, index).Result()
}

func (conn *Connection) LInsert(key, op string, pivot, value any) (int64, error) {
	return conn.client.LInsert(context.Background(), key, op, pivot, value).Result()
}

func (conn *Connection) LLen(key string) (int64, error) {
	return conn.client.LLen(context.Background(), key).Result()
}

func (conn *Connection) LPop(key string) (string, error) {
	return conn.client.LPop(context.Background(), key).Result()
}

func (conn *Connection) LPush(key string, values ...any) (int64, error) {
	return conn.client.LPush(context.Background(), key, values...).Result()
}

func (conn *Connection) LPushX(key string, values ...any) (int64, error) {
	return conn.client.LPushX(context.Background(), key, values...).Result()
}

func (conn *Connection) LRange(key string, start, stop int64) ([]string, error) {
	return conn.client.LRange(context.Background(), key, start, stop).Result()
}

func (conn *Connection) LRem(key string, count int64, value any) (int64, error) {
	return conn.client.LRem(context.Background(), key, count, value).Result()
}

func (conn *Connection) LSet(key string, index int64, value any) (string, error) {
	return conn.client.LSet(context.Background(), key, index, value).Result()
}

func (conn *Connection) LTrim(key string, start, stop int64) (string, error) {
	return conn.client.LTrim(context.Background(), key, start, stop).Result()
}

func (conn *Connection) RPop(key string) (string, error) {
	return conn.client.RPop(context.Background(), key).Result()
}

func (conn *Connection) RPopCount(key string, count int) ([]string, error) {
	return conn.client.RPopCount(context.Background(), key, count).Result()
}

func (conn *Connection) RPopLPush(source, destination string) (string, error) {
	return conn.client.RPopLPush(context.Background(), source, destination).Result()
}

func (conn *Connection) RPush(key string, values ...any) (int64, error) {
	return conn.client.RPush(context.Background(), key, values...).Result()
}

func (conn *Connection) RPushX(key string, values ...any) (int64, error) {
	return conn.client.RPushX(context.Background(), key, values...).Result()
}

// lists end

// scripting start
func (conn *Connection) Eval(script string, keys []string, args ...any) (any, error) {
	return conn.client.Eval(context.Background(), script, keys, args...).Result()
}

func (conn *Connection) EvalSha(sha1 string, keys []string, args ...any) (any, error) {
	return conn.client.EvalSha(context.Background(), sha1, keys, args...).Result()
}

func (conn *Connection) ScriptExists(hashes ...string) ([]bool, error) {
	return conn.client.ScriptExists(context.Background(), hashes...).Result()
}

func (conn *Connection) ScriptFlush() (string, error) {
	return conn.client.ScriptFlush(context.Background()).Result()
}

func (conn *Connection) ScriptKill() (string, error) {
	return conn.client.ScriptKill(context.Background()).Result()
}

func (conn *Connection) ScriptLoad(script string) (string, error) {
	return conn.client.ScriptLoad(context.Background(), script).Result()
}

// scripting end

// zset start

func (conn *Connection) ZAdd(key string, members ...*contracts.Z) (int64, error) {
	goredisMembers := make([]*goredis.Z, len(members))
	for memberKey, value := range members {
		goredisMembers[memberKey] = &goredis.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return conn.client.ZAdd(context.Background(), key, goredisMembers...).Result()
}

func (conn *Connection) ZCard(key string) (int64, error) {
	return conn.client.ZCard(context.Background(), key).Result()
}

func (conn *Connection) ZCount(key, min, max string) (int64, error) {
	return conn.client.ZCount(context.Background(), key, min, max).Result()
}

func (conn *Connection) ZIncrBy(key string, increment float64, member string) (float64, error) {
	return conn.client.ZIncrBy(context.Background(), key, increment, member).Result()
}

func (conn *Connection) ZInterStore(destination string, store *contracts.ZStore) (int64, error) {
	return conn.client.ZInterStore(context.Background(), destination, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (conn *Connection) ZLexCount(key, min, max string) (int64, error) {
	return conn.client.ZLexCount(context.Background(), key, min, max).Result()
}

func (conn *Connection) ZPopMax(key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := conn.client.ZPopMax(context.Background(), key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (conn *Connection) ZPopMin(key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := conn.client.ZPopMin(context.Background(), key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (conn *Connection) ZRange(key string, start, stop int64) ([]string, error) {
	return conn.client.ZRange(context.Background(), key, start, stop).Result()
}

func (conn *Connection) ZRangeByLex(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRangeByLex(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRevRangeByLex(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRevRangeByLex(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRangeByScore(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRangeByScore(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRank(key, member string) (int64, error) {
	return conn.client.ZRank(context.Background(), key, member).Result()
}

func (conn *Connection) ZRem(key string, members ...any) (int64, error) {
	return conn.client.ZRem(context.Background(), key, members...).Result()
}

func (conn *Connection) ZRemRangeByLex(key, min, max string) (int64, error) {
	return conn.client.ZRemRangeByLex(context.Background(), key, min, max).Result()
}

func (conn *Connection) ZRemRangeByRank(key string, start, stop int64) (int64, error) {
	return conn.client.ZRemRangeByRank(context.Background(), key, start, stop).Result()
}

func (conn *Connection) ZRemRangeByScore(key, min, max string) (int64, error) {
	return conn.client.ZRemRangeByScore(context.Background(), key, min, max).Result()
}

func (conn *Connection) ZRevRange(key string, start, stop int64) ([]string, error) {
	return conn.client.ZRevRange(context.Background(), key, start, stop).Result()
}

func (conn *Connection) ZRevRangeByScore(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRevRangeByScore(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRevRank(key, member string) (int64, error) {
	return conn.client.ZRevRank(context.Background(), key, member).Result()
}

func (conn *Connection) ZScore(key, member string) (float64, error) {
	return conn.client.ZScore(context.Background(), key, member).Result()
}

func (conn *Connection) ZUnionStore(key string, store *contracts.ZStore) (int64, error) {
	return conn.client.ZUnionStore(context.Background(), key, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (conn *Connection) ZScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.ZScan(context.Background(), key, cursor, match, count).Result()
}

// zset end

// ctx func

func (conn *Connection) SubscribeWithContext(ctx context.Context, channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := conn.client.Subscribe(ctx, channels...)
	if pingErr := pubSub.Ping(ctx, ""); pingErr != nil {
		return pingErr
	}

	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				conn.exceptionHandler.Handle(&SubscribeException{Err: err})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (conn *Connection) PSubscribeWithContext(ctx context.Context, channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := conn.client.PSubscribe(ctx, channels...)
	if pingErr := pubSub.Ping(ctx, ""); pingErr != nil {
		return pingErr
	}
	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				conn.exceptionHandler.Handle(&SubscribeException{Err: err})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (conn *Connection) CommandWithContext(ctx context.Context, method string, args ...any) (any, error) {
	return conn.client.Do(ctx, append([]any{method}, args...)...).Result()
}

func (conn *Connection) PubSubChannelsWithContext(ctx context.Context, pattern string) ([]string, error) {
	return conn.client.PubSubChannels(ctx, pattern).Result()
}

func (conn *Connection) PubSubNumSubWithContext(ctx context.Context, channels ...string) (map[string]int64, error) {
	return conn.client.PubSubNumSub(ctx, channels...).Result()
}

func (conn *Connection) PubSubNumPatWithContext(ctx context.Context) (int64, error) {
	return conn.client.PubSubNumPat(context.Background()).Result()
}

func (conn *Connection) PublishWithContext(ctx context.Context, channel string, message any) (int64, error) {
	return conn.client.Publish(ctx, channel, message).Result()
}

func (conn *Connection) ClientWithContext() *goredis.Client {
	return conn.client
}

// getter start
func (conn *Connection) GetWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.Get(ctx, key).Result()
}

func (conn *Connection) MGetWithContext(ctx context.Context, keys ...string) ([]any, error) {
	return conn.client.MGet(ctx, keys...).Result()
}

func (conn *Connection) GetBitWithContext(ctx context.Context, key string, offset int64) (int64, error) {
	return conn.client.GetBit(ctx, key, offset).Result()
}

func (conn *Connection) BitOpAndWithContext(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpAnd(ctx, destKey, keys...).Result()
}

func (conn *Connection) BitOpNotWithContext(ctx context.Context, destKey string, key string) (int64, error) {
	return conn.client.BitOpNot(ctx, destKey, key).Result()
}

func (conn *Connection) BitOpOrWithContext(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpOr(ctx, destKey, keys...).Result()
}

func (conn *Connection) BitOpXorWithContext(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return conn.client.BitOpXor(ctx, destKey, keys...).Result()
}

func (conn *Connection) GetDelWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.GetDel(ctx, key).Result()
}

func (conn *Connection) GetExWithContext(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return conn.client.GetEx(ctx, key, expiration).Result()
}

func (conn *Connection) GetRangeWithContext(ctx context.Context, key string, start, end int64) (string, error) {
	return conn.client.GetRange(ctx, key, start, end).Result()
}

func (conn *Connection) GetSetWithContext(ctx context.Context, key string, value any) (string, error) {
	return conn.client.GetSet(ctx, key, value).Result()
}

func (conn *Connection) ClientGetNameWithContext(ctx context.Context) (string, error) {
	return conn.client.ClientGetName(ctx).Result()
}

func (conn *Connection) StrLenWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.StrLen(ctx, key).Result()
}

// getter end
// keys start

func (conn *Connection) KeysWithContext(ctx context.Context, pattern string) ([]string, error) {
	return conn.client.Keys(ctx, pattern).Result()
}

func (conn *Connection) DelWithContext(ctx context.Context, keys ...string) (int64, error) {
	return conn.client.Del(ctx, keys...).Result()
}

func (conn *Connection) FlushAllWithContext(ctx context.Context) (string, error) {
	return conn.client.FlushAll(context.Background()).Result()
}

func (conn *Connection) FlushDBWithContext(ctx context.Context) (string, error) {
	return conn.client.FlushDB(ctx).Result()
}

func (conn *Connection) DumpWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.Dump(ctx, key).Result()
}

func (conn *Connection) ExistsWithContext(ctx context.Context, keys ...string) (int64, error) {
	return conn.client.Exists(ctx, keys...).Result()
}

func (conn *Connection) ExpireWithContext(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return conn.client.Expire(ctx, key, expiration).Result()
}

func (conn *Connection) ExpireAtWithContext(ctx context.Context, key string, tm time.Time) (bool, error) {
	return conn.client.ExpireAt(ctx, key, tm).Result()
}

func (conn *Connection) PExpireWithContext(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return conn.client.PExpire(ctx, key, expiration).Result()
}

func (conn *Connection) PExpireAtWithContext(ctx context.Context, key string, tm time.Time) (bool, error) {
	return conn.client.PExpireAt(ctx, key, tm).Result()
}

func (conn *Connection) MigrateWithContext(ctx context.Context, host, port, key string, db int, timeout time.Duration) (string, error) {
	return conn.client.Migrate(ctx, host, port, key, db, timeout).Result()
}

func (conn *Connection) MoveWithContext(ctx context.Context, key string, db int) (bool, error) {
	return conn.client.Move(ctx, key, db).Result()
}

func (conn *Connection) PersistWithContext(ctx context.Context, key string) (bool, error) {
	return conn.client.Persist(ctx, key).Result()
}

func (conn *Connection) PTTLWithContext(ctx context.Context, key string) (time.Duration, error) {
	return conn.client.PTTL(ctx, key).Result()
}

func (conn *Connection) TTLWithContext(ctx context.Context, key string) (time.Duration, error) {
	return conn.client.TTL(ctx, key).Result()
}

func (conn *Connection) RandomKeyWithContext(ctx context.Context) (string, error) {
	return conn.client.RandomKey(ctx).Result()
}

func (conn *Connection) RenameWithContext(ctx context.Context, key, newKey string) (string, error) {
	return conn.client.Rename(ctx, key, newKey).Result()
}

func (conn *Connection) RenameNXWithContext(ctx context.Context, key, newKey string) (bool, error) {
	return conn.client.RenameNX(ctx, key, newKey).Result()
}

func (conn *Connection) TypeWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.Type(ctx, key).Result()
}

func (conn *Connection) WaitWithContext(ctx context.Context, numSlaves int, timeout time.Duration) (int64, error) {
	return conn.client.Wait(ctx, numSlaves, timeout).Result()
}

func (conn *Connection) ScanWithContext(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.Scan(ctx, cursor, match, count).Result()
}

func (conn *Connection) BitCountWithContext(ctx context.Context, key string, count *contracts.BitCount) (int64, error) {
	return conn.client.BitCount(ctx, key, &goredis.BitCount{
		Start: count.Start,
		End:   count.End,
	}).Result()
}

// keys end

// setter start
func (conn *Connection) SetWithContext(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return conn.client.Set(ctx, key, value, expiration).Result()
}

func (conn *Connection) AppendWithContext(ctx context.Context, key, value string) (int64, error) {
	return conn.client.Append(ctx, key, value).Result()
}

func (conn *Connection) MSetWithContext(ctx context.Context, values ...any) (string, error) {
	return conn.client.MSet(ctx, values...).Result()
}

func (conn *Connection) MSetNXWithContext(ctx context.Context, values ...any) (bool, error) {
	return conn.client.MSetNX(ctx, values...).Result()
}

func (conn *Connection) SetNXWithContext(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return conn.client.SetNX(ctx, key, value, expiration).Result()
}

func (conn *Connection) SetEXWithContext(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return conn.client.SetEX(ctx, key, value, expiration).Result()
}

func (conn *Connection) SetBitWithContext(ctx context.Context, key string, offset int64, value int) (int64, error) {
	return conn.client.SetBit(ctx, key, offset, value).Result()
}

func (conn *Connection) BitPosWithContext(ctx context.Context, key string, bit int64, pos ...int64) (int64, error) {
	return conn.client.BitPos(ctx, key, bit, pos...).Result()
}

func (conn *Connection) SetRangeWithContext(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return conn.client.SetRange(ctx, key, offset, value).Result()
}

func (conn *Connection) IncrWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.Incr(ctx, key).Result()
}

func (conn *Connection) DecrWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.Decr(ctx, key).Result()
}

func (conn *Connection) IncrByWithContext(ctx context.Context, key string, value int64) (int64, error) {
	return conn.client.IncrBy(ctx, key, value).Result()
}

func (conn *Connection) DecrByWithContext(ctx context.Context, key string, value int64) (int64, error) {
	return conn.client.DecrBy(ctx, key, value).Result()
}

func (conn *Connection) IncrByFloatWithContext(ctx context.Context, key string, value float64) (float64, error) {
	return conn.client.IncrByFloat(ctx, key, value).Result()
}

// setter end

// hash start
func (conn *Connection) HGetWithContext(ctx context.Context, key, field string) (string, error) {
	return conn.client.HGet(ctx, key, field).Result()
}

func (conn *Connection) HGetAllWithContext(ctx context.Context, key string) (map[string]string, error) {
	return conn.client.HGetAll(ctx, key).Result()
}

func (conn *Connection) HMGetWithContext(ctx context.Context, key string, fields ...string) ([]any, error) {
	return conn.client.HMGet(ctx, key, fields...).Result()
}

func (conn *Connection) HKeysWithContext(ctx context.Context, key string) ([]string, error) {
	return conn.client.HKeys(ctx, key).Result()
}

func (conn *Connection) HLenWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.HLen(ctx, key).Result()
}

func (conn *Connection) HRandFieldWithContext(ctx context.Context, key string, count int, withValues bool) ([]string, error) {
	return conn.client.HRandField(ctx, key, count, withValues).Result()
}

func (conn *Connection) HScanWithContext(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.HScan(ctx, key, cursor, match, count).Result()
}

func (conn *Connection) HValuesWithContext(ctx context.Context, key string) ([]string, error) {
	return conn.client.HVals(ctx, key).Result()
}

func (conn *Connection) HSetWithContext(ctx context.Context, key string, values ...any) (int64, error) {
	return conn.client.HSet(ctx, key, values...).Result()
}

func (conn *Connection) HSetNXWithContext(ctx context.Context, key, field string, value any) (bool, error) {
	return conn.client.HSetNX(ctx, key, field, value).Result()
}

func (conn *Connection) HMSetWithContext(ctx context.Context, key string, values ...any) (bool, error) {
	return conn.client.HMSet(ctx, key, values...).Result()
}

func (conn *Connection) HDelWithContext(ctx context.Context, key string, fields ...string) (int64, error) {
	return conn.client.HDel(ctx, key, fields...).Result()
}

func (conn *Connection) HExistsWithContext(ctx context.Context, key string, field string) (bool, error) {
	return conn.client.HExists(ctx, key, field).Result()
}

func (conn *Connection) HIncrByWithContext(ctx context.Context, key string, field string, value int64) (int64, error) {
	return conn.client.HIncrBy(ctx, key, field, value).Result()
}

func (conn *Connection) HIncrByFloatWithContext(ctx context.Context, key string, field string, value float64) (float64, error) {
	return conn.client.HIncrByFloat(ctx, key, field, value).Result()
}

// hash end

// set start
func (conn *Connection) SAddWithContext(ctx context.Context, key string, members ...any) (int64, error) {
	return conn.client.SAdd(ctx, key, members...).Result()
}

func (conn *Connection) SCardWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.SCard(ctx, key).Result()
}

func (conn *Connection) SDiffWithContext(ctx context.Context, keys ...string) ([]string, error) {
	return conn.client.SDiff(ctx, keys...).Result()
}

func (conn *Connection) SDiffStoreWithContext(ctx context.Context, destination string, keys ...string) (int64, error) {
	return conn.client.SDiffStore(ctx, destination, keys...).Result()
}

func (conn *Connection) SInterWithContext(ctx context.Context, keys ...string) ([]string, error) {
	return conn.client.SInter(ctx, keys...).Result()
}

func (conn *Connection) SInterStoreWithContext(ctx context.Context, destination string, keys ...string) (int64, error) {
	return conn.client.SInterStore(ctx, destination, keys...).Result()
}

func (conn *Connection) SIsMemberWithContext(ctx context.Context, key string, member any) (bool, error) {
	return conn.client.SIsMember(ctx, key, member).Result()
}

func (conn *Connection) SMembersWithContext(ctx context.Context, key string) ([]string, error) {
	return conn.client.SMembers(ctx, key).Result()
}

func (conn *Connection) SRemWithContext(ctx context.Context, key string, members ...any) (int64, error) {
	return conn.client.SRem(ctx, key, members...).Result()
}

func (conn *Connection) SPopNWithContext(ctx context.Context, key string, count int64) ([]string, error) {
	return conn.client.SPopN(ctx, key, count).Result()
}

func (conn *Connection) SPopWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.SPop(ctx, key).Result()
}

func (conn *Connection) SRandMemberNWithContext(ctx context.Context, key string, count int64) ([]string, error) {
	return conn.client.SRandMemberN(ctx, key, count).Result()
}

func (conn *Connection) SMoveWithContext(ctx context.Context, source, destination string, member any) (bool, error) {
	return conn.client.SMove(ctx, source, destination, member).Result()
}

func (conn *Connection) SRandMemberWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.SRandMember(ctx, key).Result()
}

func (conn *Connection) SUnionWithContext(ctx context.Context, keys ...string) ([]string, error) {
	return conn.client.SUnion(ctx, keys...).Result()
}

func (conn *Connection) SUnionStoreWithContext(ctx context.Context, destination string, keys ...string) (int64, error) {
	return conn.client.SUnionStore(ctx, destination, keys...).Result()
}

// set end

// geo start

func (conn *Connection) GeoAddWithContext(ctx context.Context, key string, geoLocation ...*contracts.GeoLocation) (int64, error) {
	goredisLocations := make([]*goredis.GeoLocation, 0)
	for locationKey, value := range geoLocation {
		goredisLocations[locationKey] = &goredis.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return conn.client.GeoAdd(ctx, key, goredisLocations...).Result()
}

func (conn *Connection) GeoHashWithContext(ctx context.Context, key string, members ...string) ([]string, error) {
	return conn.client.GeoHash(ctx, key, members...).Result()
}

func (conn *Connection) GeoPosWithContext(ctx context.Context, key string, members ...string) ([]*contracts.GeoPos, error) {
	results := make([]*contracts.GeoPos, 0)
	goredisResults, err := conn.client.GeoPos(ctx, key, members...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = &contracts.GeoPos{
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
		}
	}
	return results, err
}

func (conn *Connection) GeoDistWithContext(ctx context.Context, key string, member1, member2, unit string) (float64, error) {
	return conn.client.GeoDist(ctx, key, member1, member2, unit).Result()
}

func (conn *Connection) GeoRadiusWithContext(ctx context.Context, key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := conn.client.GeoRadius(ctx, key, longitude, latitude, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return results, err
}

func (conn *Connection) GeoRadiusStoreWithContext(ctx context.Context, key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) (int64, error) {
	return conn.client.GeoRadiusStore(ctx, key, longitude, latitude, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
}

func (conn *Connection) GeoRadiusByMemberWithContext(ctx context.Context, key, member string, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := conn.client.GeoRadiusByMember(ctx, key, member, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.GeoLocation{
			Name:      value.Name,
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
			Dist:      value.Dist,
			GeoHash:   value.GeoHash,
		}
	}
	return results, err
}

func (conn *Connection) GeoRadiusByMemberStoreWithContext(ctx context.Context, key, member string, query *contracts.GeoRadiusQuery) (int64, error) {
	return conn.client.GeoRadiusByMemberStore(ctx, key, member, &goredis.GeoRadiusQuery{
		Radius:      query.Radius,
		Unit:        query.Unit,
		WithCoord:   query.WithCoord,
		WithDist:    query.WithDist,
		WithGeoHash: query.WithGeoHash,
		Count:       query.Count,
		Sort:        query.Sort,
		Store:       query.Store,
		StoreDist:   query.StoreDist,
	}).Result()
}

// geo end

// lists start

func (conn *Connection) BLPopWithContext(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return conn.client.BLPop(ctx, timeout, keys...).Result()
}

func (conn *Connection) BRPopWithContext(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return conn.client.BRPop(ctx, timeout, keys...).Result()
}

func (conn *Connection) BRPopLPushWithContext(ctx context.Context, source, destination string, timeout time.Duration) (string, error) {
	return conn.client.BRPopLPush(ctx, source, destination, timeout).Result()
}

func (conn *Connection) LIndexWithContext(ctx context.Context, key string, index int64) (string, error) {
	return conn.client.LIndex(ctx, key, index).Result()
}

func (conn *Connection) LInsertWithContext(ctx context.Context, key, op string, pivot, value any) (int64, error) {
	return conn.client.LInsert(ctx, key, op, pivot, value).Result()
}

func (conn *Connection) LLenWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.LLen(ctx, key).Result()
}

func (conn *Connection) LPopWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.LPop(ctx, key).Result()
}

func (conn *Connection) LPushWithContext(ctx context.Context, key string, values ...any) (int64, error) {
	return conn.client.LPush(ctx, key, values...).Result()
}

func (conn *Connection) LPushXWithContext(ctx context.Context, key string, values ...any) (int64, error) {
	return conn.client.LPushX(ctx, key, values...).Result()
}

func (conn *Connection) LRangeWithContext(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return conn.client.LRange(ctx, key, start, stop).Result()
}

func (conn *Connection) LRemWithContext(ctx context.Context, key string, count int64, value any) (int64, error) {
	return conn.client.LRem(ctx, key, count, value).Result()
}

func (conn *Connection) LSetWithContext(ctx context.Context, key string, index int64, value any) (string, error) {
	return conn.client.LSet(ctx, key, index, value).Result()
}

func (conn *Connection) LTrimWithContext(ctx context.Context, key string, start, stop int64) (string, error) {
	return conn.client.LTrim(ctx, key, start, stop).Result()
}

func (conn *Connection) RPopWithContext(ctx context.Context, key string) (string, error) {
	return conn.client.RPop(ctx, key).Result()
}

func (conn *Connection) RPopCountWithContext(ctx context.Context, key string, count int) ([]string, error) {
	return conn.client.RPopCount(ctx, key, count).Result()
}

func (conn *Connection) RPopLPushWithContext(ctx context.Context, source, destination string) (string, error) {
	return conn.client.RPopLPush(ctx, source, destination).Result()
}

func (conn *Connection) RPushWithContext(ctx context.Context, key string, values ...any) (int64, error) {
	return conn.client.RPush(ctx, key, values...).Result()
}

func (conn *Connection) RPushXWithContext(ctx context.Context, key string, values ...any) (int64, error) {
	return conn.client.RPushX(ctx, key, values...).Result()
}

// lists end

// scripting start
func (conn *Connection) EvalWithContext(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return conn.client.Eval(ctx, script, keys, args...).Result()
}

func (conn *Connection) EvalShaWithContext(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	return conn.client.EvalSha(ctx, sha1, keys, args...).Result()
}

func (conn *Connection) ScriptExistsWithContext(ctx context.Context, hashes ...string) ([]bool, error) {
	return conn.client.ScriptExists(ctx, hashes...).Result()
}

func (conn *Connection) ScriptFlushWithContext(ctx context.Context) (string, error) {
	return conn.client.ScriptFlush(ctx).Result()
}

func (conn *Connection) ScriptKillWithContext(ctx context.Context) (string, error) {
	return conn.client.ScriptKill(ctx).Result()
}

func (conn *Connection) ScriptLoadWithContext(ctx context.Context, script string) (string, error) {
	return conn.client.ScriptLoad(ctx, script).Result()
}

// scripting end

// zset start

func (conn *Connection) ZAddWithContext(ctx context.Context, key string, members ...*contracts.Z) (int64, error) {
	goredisMembers := make([]*goredis.Z, len(members))
	for memberKey, value := range members {
		goredisMembers[memberKey] = &goredis.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return conn.client.ZAdd(ctx, key, goredisMembers...).Result()
}

func (conn *Connection) ZCardWithContext(ctx context.Context, key string) (int64, error) {
	return conn.client.ZCard(ctx, key).Result()
}

func (conn *Connection) ZCountWithContext(ctx context.Context, key, min, max string) (int64, error) {
	return conn.client.ZCount(ctx, key, min, max).Result()
}

func (conn *Connection) ZIncrByWithContext(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return conn.client.ZIncrBy(ctx, key, increment, member).Result()
}

func (conn *Connection) ZInterStoreWithContext(ctx context.Context, destination string, store *contracts.ZStore) (int64, error) {
	return conn.client.ZInterStore(ctx, destination, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (conn *Connection) ZLexCountWithContext(ctx context.Context, key, min, max string) (int64, error) {
	return conn.client.ZLexCount(ctx, key, min, max).Result()
}

func (conn *Connection) ZPopMaxWithContext(ctx context.Context, key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := conn.client.ZPopMax(ctx, key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (conn *Connection) ZPopMinWithContext(ctx context.Context, key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := conn.client.ZPopMin(ctx, key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (conn *Connection) ZRangeWithContext(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return conn.client.ZRange(ctx, key, start, stop).Result()
}

func (conn *Connection) ZRangeByLexWithContext(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRangeByLex(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRevRangeByLexWithContext(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRevRangeByLex(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRangeByScoreWithContext(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRankWithContext(ctx context.Context, key, member string) (int64, error) {
	return conn.client.ZRank(ctx, key, member).Result()
}

func (conn *Connection) ZRemWithContext(ctx context.Context, key string, members ...any) (int64, error) {
	return conn.client.ZRem(ctx, key, members...).Result()
}

func (conn *Connection) ZRemRangeByLexWithContext(ctx context.Context, key, min, max string) (int64, error) {
	return conn.client.ZRemRangeByLex(ctx, key, min, max).Result()
}

func (conn *Connection) ZRemRangeByRankWithContext(ctx context.Context, key string, start, stop int64) (int64, error) {
	return conn.client.ZRemRangeByRank(ctx, key, start, stop).Result()
}

func (conn *Connection) ZRemRangeByScoreWithContext(ctx context.Context, key, min, max string) (int64, error) {
	return conn.client.ZRemRangeByScore(ctx, key, min, max).Result()
}

func (conn *Connection) ZRevRangeWithContext(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return conn.client.ZRevRange(ctx, key, start, stop).Result()
}

func (conn *Connection) ZRevRangeByScoreWithContext(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return conn.client.ZRevRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (conn *Connection) ZRevRankWithContext(ctx context.Context, key, member string) (int64, error) {
	return conn.client.ZRevRank(ctx, key, member).Result()
}

func (conn *Connection) ZScoreWithContext(ctx context.Context, key, member string) (float64, error) {
	return conn.client.ZScore(ctx, key, member).Result()
}

func (conn *Connection) ZUnionStoreWithContext(ctx context.Context, key string, store *contracts.ZStore) (int64, error) {
	return conn.client.ZUnionStore(ctx, key, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (conn *Connection) ZScanWithContext(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return conn.client.ZScan(ctx, key, cursor, match, count).Result()
}

// zset end
