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

func (this *Connection) Subscribe(channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := this.client.Subscribe(context.Background(), channels...)
	if pingErr := pubSub.Ping(context.Background(), ""); pingErr != nil {
		return pingErr
	}

	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {

				// 处理异常
				this.exceptionHandler.Handle(SubscribeException{
					err, nil,
				})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (this *Connection) PSubscribe(channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := this.client.PSubscribe(context.Background(), channels...)
	if pingErr := pubSub.Ping(context.Background(), ""); pingErr != nil {
		return pingErr
	}
	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				this.exceptionHandler.Handle(SubscribeException{
					err, nil,
				})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (this *Connection) Command(method string, args ...interface{}) (interface{}, error) {
	return this.client.Do(context.Background(), append([]interface{}{method}, args...)...).Result()
}

func (this *Connection) PubSubChannels(pattern string) ([]string, error) {
	return this.client.PubSubChannels(context.Background(), pattern).Result()
}

func (this *Connection) PubSubNumSub(channels ...string) (map[string]int64, error) {
	return this.client.PubSubNumSub(context.Background(), channels...).Result()
}

func (this *Connection) PubSubNumPat() (int64, error) {
	return this.client.PubSubNumPat(context.Background()).Result()
}

func (this *Connection) Publish(channel string, message interface{}) (int64, error) {
	return this.client.Publish(context.Background(), channel, message).Result()
}

func (this *Connection) Client() *goredis.Client {
	return this.client
}

// getter start
func (this *Connection) Get(key string) (string, error) {
	return this.client.Get(context.Background(), key).Result()
}

func (this *Connection) MGet(keys ...string) ([]interface{}, error) {
	return this.client.MGet(context.Background(), keys...).Result()
}

func (this *Connection) GetBit(key string, offset int64) (int64, error) {
	return this.client.GetBit(context.Background(), key, offset).Result()
}

func (this *Connection) BitOpAnd(destKey string, keys ...string) (int64, error) {
	return this.client.BitOpAnd(context.Background(), destKey, keys...).Result()
}

func (this *Connection) BitOpNot(destKey string, key string) (int64, error) {
	return this.client.BitOpNot(context.Background(), destKey, key).Result()
}

func (this *Connection) BitOpOr(destKey string, keys ...string) (int64, error) {
	return this.client.BitOpOr(context.Background(), destKey, keys...).Result()
}

func (this *Connection) BitOpXor(destKey string, keys ...string) (int64, error) {
	return this.client.BitOpXor(context.Background(), destKey, keys...).Result()
}

func (this *Connection) GetDel(key string) (string, error) {
	return this.client.GetDel(context.Background(), key).Result()
}

func (this *Connection) GetEx(key string, expiration time.Duration) (string, error) {
	return this.client.GetEx(context.Background(), key, expiration).Result()
}

func (this *Connection) GetRange(key string, start, end int64) (string, error) {
	return this.client.GetRange(context.Background(), key, start, end).Result()
}

func (this *Connection) GetSet(key string, value interface{}) (string, error) {
	return this.client.GetSet(context.Background(), key, value).Result()
}

func (this *Connection) ClientGetName() (string, error) {
	return this.client.ClientGetName(context.Background()).Result()
}

func (this *Connection) StrLen(key string) (int64, error) {
	return this.client.StrLen(context.Background(), key).Result()
}

// getter end
// keys start

func (this *Connection) Keys(pattern string) ([]string, error) {
	return this.client.Keys(context.Background(), pattern).Result()
}

func (this *Connection) Del(keys ...string) (int64, error) {
	return this.client.Del(context.Background(), keys...).Result()
}

func (this *Connection) FlushAll() (string, error) {
	return this.client.FlushAll(context.Background()).Result()
}

func (this *Connection) FlushDB() (string, error) {
	return this.client.FlushDB(context.Background()).Result()
}

func (this *Connection) Dump(key string) (string, error) {
	return this.client.Dump(context.Background(), key).Result()
}

func (this *Connection) Exists(keys ...string) (int64, error) {
	return this.client.Exists(context.Background(), keys...).Result()
}

func (this *Connection) Expire(key string, expiration time.Duration) (bool, error) {
	return this.client.Expire(context.Background(), key, expiration).Result()
}

func (this *Connection) ExpireAt(key string, tm time.Time) (bool, error) {
	return this.client.ExpireAt(context.Background(), key, tm).Result()
}

func (this *Connection) PExpire(key string, expiration time.Duration) (bool, error) {
	return this.client.PExpire(context.Background(), key, expiration).Result()
}

func (this *Connection) PExpireAt(key string, tm time.Time) (bool, error) {
	return this.client.PExpireAt(context.Background(), key, tm).Result()
}

func (this *Connection) Migrate(host, port, key string, db int, timeout time.Duration) (string, error) {
	return this.client.Migrate(context.Background(), host, port, key, db, timeout).Result()
}

func (this *Connection) Move(key string, db int) (bool, error) {
	return this.client.Move(context.Background(), key, db).Result()
}

func (this *Connection) Persist(key string) (bool, error) {
	return this.client.Persist(context.Background(), key).Result()
}

func (this *Connection) PTTL(key string) (time.Duration, error) {
	return this.client.PTTL(context.Background(), key).Result()
}

func (this *Connection) TTL(key string) (time.Duration, error) {
	return this.client.TTL(context.Background(), key).Result()
}

func (this *Connection) RandomKey() (string, error) {
	return this.client.RandomKey(context.Background()).Result()
}

func (this *Connection) Rename(key, newKey string) (string, error) {
	return this.client.Rename(context.Background(), key, newKey).Result()
}

func (this *Connection) RenameNX(key, newKey string) (bool, error) {
	return this.client.RenameNX(context.Background(), key, newKey).Result()
}

func (this *Connection) Type(key string) (string, error) {
	return this.client.Type(context.Background(), key).Result()
}

func (this *Connection) Wait(numSlaves int, timeout time.Duration) (int64, error) {
	return this.client.Wait(context.Background(), numSlaves, timeout).Result()
}

func (this *Connection) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.Scan(context.Background(), cursor, match, count).Result()
}

func (this *Connection) BitCount(key string, count *contracts.BitCount) (int64, error) {
	return this.client.BitCount(context.Background(), key, &goredis.BitCount{
		Start: count.Start,
		End:   count.End,
	}).Result()
}

// keys end

// setter start
func (this *Connection) Set(key string, value interface{}, expiration time.Duration) (string, error) {
	return this.client.Set(context.Background(), key, value, expiration).Result()
}

func (this *Connection) Append(key, value string) (int64, error) {
	return this.client.Append(context.Background(), key, value).Result()
}

func (this *Connection) MSet(values ...interface{}) (string, error) {
	return this.client.MSet(context.Background(), values...).Result()
}

func (this *Connection) MSetNX(values ...interface{}) (bool, error) {
	return this.client.MSetNX(context.Background(), values...).Result()
}

func (this *Connection) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	return this.client.SetNX(context.Background(), key, value, expiration).Result()
}

func (this *Connection) SetEX(key string, value interface{}, expiration time.Duration) (string, error) {
	return this.client.SetEX(context.Background(), key, value, expiration).Result()
}

func (this *Connection) SetBit(key string, offset int64, value int) (int64, error) {
	return this.client.SetBit(context.Background(), key, offset, value).Result()
}

func (this *Connection) BitPos(key string, bit int64, pos ...int64) (int64, error) {
	return this.client.BitPos(context.Background(), key, bit, pos...).Result()
}

func (this *Connection) SetRange(key string, offset int64, value string) (int64, error) {
	return this.client.SetRange(context.Background(), key, offset, value).Result()
}

func (this *Connection) Incr(key string) (int64, error) {
	return this.client.Incr(context.Background(), key).Result()
}

func (this *Connection) Decr(key string) (int64, error) {
	return this.client.Decr(context.Background(), key).Result()
}

func (this *Connection) IncrBy(key string, value int64) (int64, error) {
	return this.client.IncrBy(context.Background(), key, value).Result()
}

func (this *Connection) DecrBy(key string, value int64) (int64, error) {
	return this.client.DecrBy(context.Background(), key, value).Result()
}

func (this *Connection) IncrByFloat(key string, value float64) (float64, error) {
	return this.client.IncrByFloat(context.Background(), key, value).Result()
}

// setter end

// hash start
func (this *Connection) HGet(key, field string) (string, error) {
	return this.client.HGet(context.Background(), key, field).Result()
}

func (this *Connection) HGetAll(key string) (map[string]string, error) {
	return this.client.HGetAll(context.Background(), key).Result()
}

func (this *Connection) HMGet(key string, fields ...string) ([]interface{}, error) {
	return this.client.HMGet(context.Background(), key, fields...).Result()
}

func (this *Connection) HKeys(key string) ([]string, error) {
	return this.client.HKeys(context.Background(), key).Result()
}

func (this *Connection) HLen(key string) (int64, error) {
	return this.client.HLen(context.Background(), key).Result()
}

func (this *Connection) HRandField(key string, count int, withValues bool) ([]string, error) {
	return this.client.HRandField(context.Background(), key, count, withValues).Result()
}

func (this *Connection) HScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.HScan(context.Background(), key, cursor, match, count).Result()
}

func (this *Connection) HValues(key string) ([]string, error) {
	return this.client.HVals(context.Background(), key).Result()
}

func (this *Connection) HSet(key string, values ...interface{}) (int64, error) {
	return this.client.HSet(context.Background(), key, values...).Result()
}

func (this *Connection) HSetNX(key, field string, value interface{}) (bool, error) {
	return this.client.HSetNX(context.Background(), key, field, value).Result()
}

func (this *Connection) HMSet(key string, values ...interface{}) (bool, error) {
	return this.client.HMSet(context.Background(), key, values...).Result()
}

func (this *Connection) HDel(key string, fields ...string) (int64, error) {
	return this.client.HDel(context.Background(), key, fields...).Result()
}

func (this *Connection) HExists(key string, field string) (bool, error) {
	return this.client.HExists(context.Background(), key, field).Result()
}

func (this *Connection) HIncrBy(key string, field string, value int64) (int64, error) {
	return this.client.HIncrBy(context.Background(), key, field, value).Result()
}

func (this *Connection) HIncrByFloat(key string, field string, value float64) (float64, error) {
	return this.client.HIncrByFloat(context.Background(), key, field, value).Result()
}

// hash end

// set start
func (this *Connection) SAdd(key string, members ...interface{}) (int64, error) {
	return this.client.SAdd(context.Background(), key, members...).Result()
}

func (this *Connection) SCard(key string) (int64, error) {
	return this.client.SCard(context.Background(), key).Result()
}

func (this *Connection) SDiff(keys ...string) ([]string, error) {
	return this.client.SDiff(context.Background(), keys...).Result()
}

func (this *Connection) SDiffStore(destination string, keys ...string) (int64, error) {
	return this.client.SDiffStore(context.Background(), destination, keys...).Result()
}

func (this *Connection) SInter(keys ...string) ([]string, error) {
	return this.client.SInter(context.Background(), keys...).Result()
}

func (this *Connection) SInterStore(destination string, keys ...string) (int64, error) {
	return this.client.SInterStore(context.Background(), destination, keys...).Result()
}

func (this *Connection) SIsMember(key string, member interface{}) (bool, error) {
	return this.client.SIsMember(context.Background(), key, member).Result()
}

func (this *Connection) SMembers(key string) ([]string, error) {
	return this.client.SMembers(context.Background(), key).Result()
}

func (this *Connection) SRem(key string, members ...interface{}) (int64, error) {
	return this.client.SRem(context.Background(), key, members...).Result()
}

func (this *Connection) SPopN(key string, count int64) ([]string, error) {
	return this.client.SPopN(context.Background(), key, count).Result()
}

func (this *Connection) SPop(key string) (string, error) {
	return this.client.SPop(context.Background(), key).Result()
}

func (this *Connection) SRandMemberN(key string, count int64) ([]string, error) {
	return this.client.SRandMemberN(context.Background(), key, count).Result()
}

func (this *Connection) SMove(source, destination string, member interface{}) (bool, error) {
	return this.client.SMove(context.Background(), source, destination, member).Result()
}

func (this *Connection) SRandMember(key string) (string, error) {
	return this.client.SRandMember(context.Background(), key).Result()
}

func (this *Connection) SUnion(keys ...string) ([]string, error) {
	return this.client.SUnion(context.Background(), keys...).Result()
}

func (this *Connection) SUnionStore(destination string, keys ...string) (int64, error) {
	return this.client.SUnionStore(context.Background(), destination, keys...).Result()
}

// set end

// geo start

func (this *Connection) GeoAdd(key string, geoLocation ...*contracts.GeoLocation) (int64, error) {
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
	return this.client.GeoAdd(context.Background(), key, goredisLocations...).Result()
}

func (this *Connection) GeoHash(key string, members ...string) ([]string, error) {
	return this.client.GeoHash(context.Background(), key, members...).Result()
}

func (this *Connection) GeoPos(key string, members ...string) ([]*contracts.GeoPos, error) {
	results := make([]*contracts.GeoPos, 0)
	goredisResults, err := this.client.GeoPos(context.Background(), key, members...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = &contracts.GeoPos{
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
		}
	}
	return results, err
}

func (this *Connection) GeoDist(key string, member1, member2, unit string) (float64, error) {
	return this.client.GeoDist(context.Background(), key, member1, member2, unit).Result()
}

func (this *Connection) GeoRadius(key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := this.client.GeoRadius(context.Background(), key, longitude, latitude, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusStore(key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) (int64, error) {
	return this.client.GeoRadiusStore(context.Background(), key, longitude, latitude, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusByMember(key, member string, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := this.client.GeoRadiusByMember(context.Background(), key, member, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusByMemberStore(key, member string, query *contracts.GeoRadiusQuery) (int64, error) {
	return this.client.GeoRadiusByMemberStore(context.Background(), key, member, &goredis.GeoRadiusQuery{
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

func (this *Connection) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return this.client.BLPop(context.Background(), timeout, keys...).Result()
}

func (this *Connection) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	return this.client.BRPop(context.Background(), timeout, keys...).Result()
}

func (this *Connection) BRPopLPush(source, destination string, timeout time.Duration) (string, error) {
	return this.client.BRPopLPush(context.Background(), source, destination, timeout).Result()
}

func (this *Connection) LIndex(key string, index int64) (string, error) {
	return this.client.LIndex(context.Background(), key, index).Result()
}

func (this *Connection) LInsert(key, op string, pivot, value interface{}) (int64, error) {
	return this.client.LInsert(context.Background(), key, op, pivot, value).Result()
}

func (this *Connection) LLen(key string) (int64, error) {
	return this.client.LLen(context.Background(), key).Result()
}

func (this *Connection) LPop(key string) (string, error) {
	return this.client.LPop(context.Background(), key).Result()
}

func (this *Connection) LPush(key string, values ...interface{}) (int64, error) {
	return this.client.LPush(context.Background(), key, values...).Result()
}

func (this *Connection) LPushX(key string, values ...interface{}) (int64, error) {
	return this.client.LPushX(context.Background(), key, values...).Result()
}

func (this *Connection) LRange(key string, start, stop int64) ([]string, error) {
	return this.client.LRange(context.Background(), key, start, stop).Result()
}

func (this *Connection) LRem(key string, count int64, value interface{}) (int64, error) {
	return this.client.LRem(context.Background(), key, count, value).Result()
}

func (this *Connection) LSet(key string, index int64, value interface{}) (string, error) {
	return this.client.LSet(context.Background(), key, index, value).Result()
}

func (this *Connection) LTrim(key string, start, stop int64) (string, error) {
	return this.client.LTrim(context.Background(), key, start, stop).Result()
}

func (this *Connection) RPop(key string) (string, error) {
	return this.client.RPop(context.Background(), key).Result()
}

func (this *Connection) RPopCount(key string, count int) ([]string, error) {
	return this.client.RPopCount(context.Background(), key, count).Result()
}

func (this *Connection) RPopLPush(source, destination string) (string, error) {
	return this.client.RPopLPush(context.Background(), source, destination).Result()
}

func (this *Connection) RPush(key string, values ...interface{}) (int64, error) {
	return this.client.RPush(context.Background(), key, values...).Result()
}

func (this *Connection) RPushX(key string, values ...interface{}) (int64, error) {
	return this.client.RPushX(context.Background(), key, values...).Result()
}

// lists end

// scripting start
func (this *Connection) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	return this.client.Eval(context.Background(), script, keys, args...).Result()
}

func (this *Connection) EvalSha(sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	return this.client.EvalSha(context.Background(), sha1, keys, args...).Result()
}

func (this *Connection) ScriptExists(hashes ...string) ([]bool, error) {
	return this.client.ScriptExists(context.Background(), hashes...).Result()
}

func (this *Connection) ScriptFlush() (string, error) {
	return this.client.ScriptFlush(context.Background()).Result()
}

func (this *Connection) ScriptKill() (string, error) {
	return this.client.ScriptKill(context.Background()).Result()
}

func (this *Connection) ScriptLoad(script string) (string, error) {
	return this.client.ScriptLoad(context.Background(), script).Result()
}

// scripting end

// zset start

func (this *Connection) ZAdd(key string, members ...*contracts.Z) (int64, error) {
	goredisMembers := make([]*goredis.Z, 0)
	for memberKey, value := range members {
		goredisMembers[memberKey] = &goredis.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return this.client.ZAdd(context.Background(), key, goredisMembers...).Result()
}

func (this *Connection) ZCard(key string) (int64, error) {
	return this.client.ZCard(context.Background(), key).Result()
}

func (this *Connection) ZCount(key, min, max string) (int64, error) {
	return this.client.ZCount(context.Background(), key, min, max).Result()
}

func (this *Connection) ZIncrBy(key string, increment float64, member string) (float64, error) {
	return this.client.ZIncrBy(context.Background(), key, increment, member).Result()
}

func (this *Connection) ZInterStore(destination string, store *contracts.ZStore) (int64, error) {
	return this.client.ZInterStore(context.Background(), destination, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (this *Connection) ZLexCount(key, min, max string) (int64, error) {
	return this.client.ZLexCount(context.Background(), key, min, max).Result()
}

func (this *Connection) ZPopMax(key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := this.client.ZPopMax(context.Background(), key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (this *Connection) ZPopMin(key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := this.client.ZPopMin(context.Background(), key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (this *Connection) ZRange(key string, start, stop int64) ([]string, error) {
	return this.client.ZRange(context.Background(), key, start, stop).Result()
}

func (this *Connection) ZRangeByLex(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRangeByLex(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRevRangeByLex(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRevRangeByLex(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRangeByScore(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRangeByScore(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRank(key, member string) (int64, error) {
	return this.client.ZRank(context.Background(), key, member).Result()
}

func (this *Connection) ZRem(key string, members ...interface{}) (int64, error) {
	return this.client.ZRem(context.Background(), key, members...).Result()
}

func (this *Connection) ZRemRangeByLex(key, min, max string) (int64, error) {
	return this.client.ZRemRangeByLex(context.Background(), key, min, max).Result()
}

func (this *Connection) ZRemRangeByRank(key string, start, stop int64) (int64, error) {
	return this.client.ZRemRangeByRank(context.Background(), key, start, stop).Result()
}

func (this *Connection) ZRemRangeByScore(key, min, max string) (int64, error) {
	return this.client.ZRemRangeByScore(context.Background(), key, min, max).Result()
}

func (this *Connection) ZRevRange(key string, start, stop int64) ([]string, error) {
	return this.client.ZRevRange(context.Background(), key, start, stop).Result()
}

func (this *Connection) ZRevRangeByScore(key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRevRangeByScore(context.Background(), key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRevRank(key, member string) (int64, error) {
	return this.client.ZRevRank(context.Background(), key, member).Result()
}

func (this *Connection) ZScore(key, member string) (float64, error) {
	return this.client.ZScore(context.Background(), key, member).Result()
}

func (this *Connection) ZUnionStore(key string, store *contracts.ZStore) (int64, error) {
	return this.client.ZUnionStore(context.Background(), key, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (this *Connection) ZScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.ZScan(context.Background(), key, cursor, match, count).Result()
}

// zset end

// ctx func

func (this *Connection) SubscribeCtx(ctx context.Context, channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := this.client.Subscribe(ctx, channels...)
	if pingErr := pubSub.Ping(ctx, ""); pingErr != nil {
		return pingErr
	}

	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				this.exceptionHandler.Handle(SubscribeException{
					err, nil,
				})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (this *Connection) PSubscribeCtx(ctx context.Context, channels []string, closure contracts.RedisSubscribeFunc) error {
	pubSub := this.client.PSubscribe(ctx, channels...)
	if pingErr := pubSub.Ping(ctx, ""); pingErr != nil {
		return pingErr
	}
	go func() {

		defer func(pubSub *goredis.PubSub) {
			err := pubSub.Close()
			if err != nil {
				// 处理异常
				this.exceptionHandler.Handle(SubscribeException{
					err, nil,
				})
			}
		}(pubSub)

		pubSubChannel := pubSub.Channel()

		for msg := range pubSubChannel {
			closure(msg.Payload, msg.Channel)
		}
	}()
	return nil
}

func (this *Connection) CommandCtx(ctx context.Context, method string, args ...interface{}) (interface{}, error) {
	return this.client.Do(ctx, append([]interface{}{method}, args...)...).Result()
}

func (this *Connection) PubSubChannelsCtx(ctx context.Context, pattern string) ([]string, error) {
	return this.client.PubSubChannels(ctx, pattern).Result()
}

func (this *Connection) PubSubNumSubCtx(ctx context.Context, channels ...string) (map[string]int64, error) {
	return this.client.PubSubNumSub(ctx, channels...).Result()
}

func (this *Connection) PubSubNumPatCtx(ctx context.Context) (int64, error) {
	return this.client.PubSubNumPat(context.Background()).Result()
}

func (this *Connection) PublishCtx(ctx context.Context, channel string, message interface{}) (int64, error) {
	return this.client.Publish(ctx, channel, message).Result()
}

func (this *Connection) ClientCtx() *goredis.Client {
	return this.client
}

// getter start
func (this *Connection) GetCtx(ctx context.Context, key string) (string, error) {
	return this.client.Get(ctx, key).Result()
}

func (this *Connection) MGetCtx(ctx context.Context, keys ...string) ([]interface{}, error) {
	return this.client.MGet(ctx, keys...).Result()
}

func (this *Connection) GetBitCtx(ctx context.Context, key string, offset int64) (int64, error) {
	return this.client.GetBit(ctx, key, offset).Result()
}

func (this *Connection) BitOpAndCtx(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return this.client.BitOpAnd(ctx, destKey, keys...).Result()
}

func (this *Connection) BitOpNotCtx(ctx context.Context, destKey string, key string) (int64, error) {
	return this.client.BitOpNot(ctx, destKey, key).Result()
}

func (this *Connection) BitOpOrCtx(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return this.client.BitOpOr(ctx, destKey, keys...).Result()
}

func (this *Connection) BitOpXorCtx(ctx context.Context, destKey string, keys ...string) (int64, error) {
	return this.client.BitOpXor(ctx, destKey, keys...).Result()
}

func (this *Connection) GetDelCtx(ctx context.Context, key string) (string, error) {
	return this.client.GetDel(ctx, key).Result()
}

func (this *Connection) GetExCtx(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return this.client.GetEx(ctx, key, expiration).Result()
}

func (this *Connection) GetRangeCtx(ctx context.Context, key string, start, end int64) (string, error) {
	return this.client.GetRange(ctx, key, start, end).Result()
}

func (this *Connection) GetSetCtx(ctx context.Context, key string, value interface{}) (string, error) {
	return this.client.GetSet(ctx, key, value).Result()
}

func (this *Connection) ClientGetNameCtx(ctx context.Context) (string, error) {
	return this.client.ClientGetName(ctx).Result()
}

func (this *Connection) StrLenCtx(ctx context.Context, key string) (int64, error) {
	return this.client.StrLen(ctx, key).Result()
}

// getter end
// keys start

func (this *Connection) KeysCtx(ctx context.Context, pattern string) ([]string, error) {
	return this.client.Keys(ctx, pattern).Result()
}

func (this *Connection) DelCtx(ctx context.Context, keys ...string) (int64, error) {
	return this.client.Del(ctx, keys...).Result()
}

func (this *Connection) FlushAllCtx(ctx context.Context) (string, error) {
	return this.client.FlushAll(context.Background()).Result()
}

func (this *Connection) FlushDBCtx(ctx context.Context) (string, error) {
	return this.client.FlushDB(ctx).Result()
}

func (this *Connection) DumpCtx(ctx context.Context, key string) (string, error) {
	return this.client.Dump(ctx, key).Result()
}

func (this *Connection) ExistsCtx(ctx context.Context, keys ...string) (int64, error) {
	return this.client.Exists(ctx, keys...).Result()
}

func (this *Connection) ExpireCtx(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return this.client.Expire(ctx, key, expiration).Result()
}

func (this *Connection) ExpireAtCtx(ctx context.Context, key string, tm time.Time) (bool, error) {
	return this.client.ExpireAt(ctx, key, tm).Result()
}

func (this *Connection) PExpireCtx(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return this.client.PExpire(ctx, key, expiration).Result()
}

func (this *Connection) PExpireAtCtx(ctx context.Context, key string, tm time.Time) (bool, error) {
	return this.client.PExpireAt(ctx, key, tm).Result()
}

func (this *Connection) MigrateCtx(ctx context.Context, host, port, key string, db int, timeout time.Duration) (string, error) {
	return this.client.Migrate(ctx, host, port, key, db, timeout).Result()
}

func (this *Connection) MoveCtx(ctx context.Context, key string, db int) (bool, error) {
	return this.client.Move(ctx, key, db).Result()
}

func (this *Connection) PersistCtx(ctx context.Context, key string) (bool, error) {
	return this.client.Persist(ctx, key).Result()
}

func (this *Connection) PTTLCtx(ctx context.Context, key string) (time.Duration, error) {
	return this.client.PTTL(ctx, key).Result()
}

func (this *Connection) TTLCtx(ctx context.Context, key string) (time.Duration, error) {
	return this.client.TTL(ctx, key).Result()
}

func (this *Connection) RandomKeyCtx(ctx context.Context) (string, error) {
	return this.client.RandomKey(ctx).Result()
}

func (this *Connection) RenameCtx(ctx context.Context, key, newKey string) (string, error) {
	return this.client.Rename(ctx, key, newKey).Result()
}

func (this *Connection) RenameNXCtx(ctx context.Context, key, newKey string) (bool, error) {
	return this.client.RenameNX(ctx, key, newKey).Result()
}

func (this *Connection) TypeCtx(ctx context.Context, key string) (string, error) {
	return this.client.Type(ctx, key).Result()
}

func (this *Connection) WaitCtx(ctx context.Context, numSlaves int, timeout time.Duration) (int64, error) {
	return this.client.Wait(ctx, numSlaves, timeout).Result()
}

func (this *Connection) ScanCtx(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.Scan(ctx, cursor, match, count).Result()
}

func (this *Connection) BitCountCtx(ctx context.Context, key string, count *contracts.BitCount) (int64, error) {
	return this.client.BitCount(ctx, key, &goredis.BitCount{
		Start: count.Start,
		End:   count.End,
	}).Result()
}

// keys end

// setter start
func (this *Connection) SetCtx(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	return this.client.Set(ctx, key, value, expiration).Result()
}

func (this *Connection) AppendCtx(ctx context.Context, key, value string) (int64, error) {
	return this.client.Append(ctx, key, value).Result()
}

func (this *Connection) MSetCtx(ctx context.Context, values ...interface{}) (string, error) {
	return this.client.MSet(ctx, values...).Result()
}

func (this *Connection) MSetNXCtx(ctx context.Context, values ...interface{}) (bool, error) {
	return this.client.MSetNX(ctx, values...).Result()
}

func (this *Connection) SetNXCtx(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return this.client.SetNX(ctx, key, value, expiration).Result()
}

func (this *Connection) SetEXCtx(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	return this.client.SetEX(ctx, key, value, expiration).Result()
}

func (this *Connection) SetBitCtx(ctx context.Context, key string, offset int64, value int) (int64, error) {
	return this.client.SetBit(ctx, key, offset, value).Result()
}

func (this *Connection) BitPosCtx(ctx context.Context, key string, bit int64, pos ...int64) (int64, error) {
	return this.client.BitPos(ctx, key, bit, pos...).Result()
}

func (this *Connection) SetRangeCtx(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return this.client.SetRange(ctx, key, offset, value).Result()
}

func (this *Connection) IncrCtx(ctx context.Context, key string) (int64, error) {
	return this.client.Incr(ctx, key).Result()
}

func (this *Connection) DecrCtx(ctx context.Context, key string) (int64, error) {
	return this.client.Decr(ctx, key).Result()
}

func (this *Connection) IncrByCtx(ctx context.Context, key string, value int64) (int64, error) {
	return this.client.IncrBy(ctx, key, value).Result()
}

func (this *Connection) DecrByCtx(ctx context.Context, key string, value int64) (int64, error) {
	return this.client.DecrBy(ctx, key, value).Result()
}

func (this *Connection) IncrByFloatCtx(ctx context.Context, key string, value float64) (float64, error) {
	return this.client.IncrByFloat(ctx, key, value).Result()
}

// setter end

// hash start
func (this *Connection) HGetCtx(ctx context.Context, key, field string) (string, error) {
	return this.client.HGet(ctx, key, field).Result()
}

func (this *Connection) HGetAllCtx(ctx context.Context, key string) (map[string]string, error) {
	return this.client.HGetAll(ctx, key).Result()
}

func (this *Connection) HMGetCtx(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	return this.client.HMGet(ctx, key, fields...).Result()
}

func (this *Connection) HKeysCtx(ctx context.Context, key string) ([]string, error) {
	return this.client.HKeys(ctx, key).Result()
}

func (this *Connection) HLenCtx(ctx context.Context, key string) (int64, error) {
	return this.client.HLen(ctx, key).Result()
}

func (this *Connection) HRandFieldCtx(ctx context.Context, key string, count int, withValues bool) ([]string, error) {
	return this.client.HRandField(ctx, key, count, withValues).Result()
}

func (this *Connection) HScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.HScan(ctx, key, cursor, match, count).Result()
}

func (this *Connection) HValuesCtx(ctx context.Context, key string) ([]string, error) {
	return this.client.HVals(ctx, key).Result()
}

func (this *Connection) HSetCtx(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return this.client.HSet(ctx, key, values...).Result()
}

func (this *Connection) HSetNXCtx(ctx context.Context, key, field string, value interface{}) (bool, error) {
	return this.client.HSetNX(ctx, key, field, value).Result()
}

func (this *Connection) HMSetCtx(ctx context.Context, key string, values ...interface{}) (bool, error) {
	return this.client.HMSet(ctx, key, values...).Result()
}

func (this *Connection) HDelCtx(ctx context.Context, key string, fields ...string) (int64, error) {
	return this.client.HDel(ctx, key, fields...).Result()
}

func (this *Connection) HExistsCtx(ctx context.Context, key string, field string) (bool, error) {
	return this.client.HExists(ctx, key, field).Result()
}

func (this *Connection) HIncrByCtx(ctx context.Context, key string, field string, value int64) (int64, error) {
	return this.client.HIncrBy(ctx, key, field, value).Result()
}

func (this *Connection) HIncrByFloatCtx(ctx context.Context, key string, field string, value float64) (float64, error) {
	return this.client.HIncrByFloat(ctx, key, field, value).Result()
}

// hash end

// set start
func (this *Connection) SAddCtx(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return this.client.SAdd(ctx, key, members...).Result()
}

func (this *Connection) SCardCtx(ctx context.Context, key string) (int64, error) {
	return this.client.SCard(ctx, key).Result()
}

func (this *Connection) SDiffCtx(ctx context.Context, keys ...string) ([]string, error) {
	return this.client.SDiff(ctx, keys...).Result()
}

func (this *Connection) SDiffStoreCtx(ctx context.Context, destination string, keys ...string) (int64, error) {
	return this.client.SDiffStore(ctx, destination, keys...).Result()
}

func (this *Connection) SInterCtx(ctx context.Context, keys ...string) ([]string, error) {
	return this.client.SInter(ctx, keys...).Result()
}

func (this *Connection) SInterStoreCtx(ctx context.Context, destination string, keys ...string) (int64, error) {
	return this.client.SInterStore(ctx, destination, keys...).Result()
}

func (this *Connection) SIsMemberCtx(ctx context.Context, key string, member interface{}) (bool, error) {
	return this.client.SIsMember(ctx, key, member).Result()
}

func (this *Connection) SMembersCtx(ctx context.Context, key string) ([]string, error) {
	return this.client.SMembers(ctx, key).Result()
}

func (this *Connection) SRemCtx(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return this.client.SRem(ctx, key, members...).Result()
}

func (this *Connection) SPopNCtx(ctx context.Context, key string, count int64) ([]string, error) {
	return this.client.SPopN(ctx, key, count).Result()
}

func (this *Connection) SPopCtx(ctx context.Context, key string) (string, error) {
	return this.client.SPop(ctx, key).Result()
}

func (this *Connection) SRandMemberNCtx(ctx context.Context, key string, count int64) ([]string, error) {
	return this.client.SRandMemberN(ctx, key, count).Result()
}

func (this *Connection) SMoveCtx(ctx context.Context, source, destination string, member interface{}) (bool, error) {
	return this.client.SMove(ctx, source, destination, member).Result()
}

func (this *Connection) SRandMemberCtx(ctx context.Context, key string) (string, error) {
	return this.client.SRandMember(ctx, key).Result()
}

func (this *Connection) SUnionCtx(ctx context.Context, keys ...string) ([]string, error) {
	return this.client.SUnion(ctx, keys...).Result()
}

func (this *Connection) SUnionStoreCtx(ctx context.Context, destination string, keys ...string) (int64, error) {
	return this.client.SUnionStore(ctx, destination, keys...).Result()
}

// set end

// geo start

func (this *Connection) GeoAddCtx(ctx context.Context, key string, geoLocation ...*contracts.GeoLocation) (int64, error) {
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
	return this.client.GeoAdd(ctx, key, goredisLocations...).Result()
}

func (this *Connection) GeoHashCtx(ctx context.Context, key string, members ...string) ([]string, error) {
	return this.client.GeoHash(ctx, key, members...).Result()
}

func (this *Connection) GeoPosCtx(ctx context.Context, key string, members ...string) ([]*contracts.GeoPos, error) {
	results := make([]*contracts.GeoPos, 0)
	goredisResults, err := this.client.GeoPos(ctx, key, members...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = &contracts.GeoPos{
			Longitude: value.Longitude,
			Latitude:  value.Latitude,
		}
	}
	return results, err
}

func (this *Connection) GeoDistCtx(ctx context.Context, key string, member1, member2, unit string) (float64, error) {
	return this.client.GeoDist(ctx, key, member1, member2, unit).Result()
}

func (this *Connection) GeoRadiusCtx(ctx context.Context, key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := this.client.GeoRadius(ctx, key, longitude, latitude, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusStoreCtx(ctx context.Context, key string, longitude, latitude float64, query *contracts.GeoRadiusQuery) (int64, error) {
	return this.client.GeoRadiusStore(ctx, key, longitude, latitude, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusByMemberCtx(ctx context.Context, key, member string, query *contracts.GeoRadiusQuery) ([]contracts.GeoLocation, error) {
	results := make([]contracts.GeoLocation, 0)
	goredisResults, err := this.client.GeoRadiusByMember(ctx, key, member, &goredis.GeoRadiusQuery{
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

func (this *Connection) GeoRadiusByMemberStoreCtx(ctx context.Context, key, member string, query *contracts.GeoRadiusQuery) (int64, error) {
	return this.client.GeoRadiusByMemberStore(ctx, key, member, &goredis.GeoRadiusQuery{
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

func (this *Connection) BLPopCtx(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return this.client.BLPop(ctx, timeout, keys...).Result()
}

func (this *Connection) BRPopCtx(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return this.client.BRPop(ctx, timeout, keys...).Result()
}

func (this *Connection) BRPopLPushCtx(ctx context.Context, source, destination string, timeout time.Duration) (string, error) {
	return this.client.BRPopLPush(ctx, source, destination, timeout).Result()
}

func (this *Connection) LIndexCtx(ctx context.Context, key string, index int64) (string, error) {
	return this.client.LIndex(ctx, key, index).Result()
}

func (this *Connection) LInsertCtx(ctx context.Context, key, op string, pivot, value interface{}) (int64, error) {
	return this.client.LInsert(ctx, key, op, pivot, value).Result()
}

func (this *Connection) LLenCtx(ctx context.Context, key string) (int64, error) {
	return this.client.LLen(ctx, key).Result()
}

func (this *Connection) LPopCtx(ctx context.Context, key string) (string, error) {
	return this.client.LPop(ctx, key).Result()
}

func (this *Connection) LPushCtx(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return this.client.LPush(ctx, key, values...).Result()
}

func (this *Connection) LPushXCtx(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return this.client.LPushX(ctx, key, values...).Result()
}

func (this *Connection) LRangeCtx(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return this.client.LRange(ctx, key, start, stop).Result()
}

func (this *Connection) LRemCtx(ctx context.Context, key string, count int64, value interface{}) (int64, error) {
	return this.client.LRem(ctx, key, count, value).Result()
}

func (this *Connection) LSetCtx(ctx context.Context, key string, index int64, value interface{}) (string, error) {
	return this.client.LSet(ctx, key, index, value).Result()
}

func (this *Connection) LTrimCtx(ctx context.Context, key string, start, stop int64) (string, error) {
	return this.client.LTrim(ctx, key, start, stop).Result()
}

func (this *Connection) RPopCtx(ctx context.Context, key string) (string, error) {
	return this.client.RPop(ctx, key).Result()
}

func (this *Connection) RPopCountCtx(ctx context.Context, key string, count int) ([]string, error) {
	return this.client.RPopCount(ctx, key, count).Result()
}

func (this *Connection) RPopLPushCtx(ctx context.Context, source, destination string) (string, error) {
	return this.client.RPopLPush(ctx, source, destination).Result()
}

func (this *Connection) RPushCtx(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return this.client.RPush(ctx, key, values...).Result()
}

func (this *Connection) RPushXCtx(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return this.client.RPushX(ctx, key, values...).Result()
}

// lists end

// scripting start
func (this *Connection) EvalCtx(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return this.client.Eval(ctx, script, keys, args...).Result()
}

func (this *Connection) EvalShaCtx(ctx context.Context, sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	return this.client.EvalSha(ctx, sha1, keys, args...).Result()
}

func (this *Connection) ScriptExistsCtx(ctx context.Context, hashes ...string) ([]bool, error) {
	return this.client.ScriptExists(ctx, hashes...).Result()
}

func (this *Connection) ScriptFlushCtx(ctx context.Context) (string, error) {
	return this.client.ScriptFlush(ctx).Result()
}

func (this *Connection) ScriptKillCtx(ctx context.Context) (string, error) {
	return this.client.ScriptKill(ctx).Result()
}

func (this *Connection) ScriptLoadCtx(ctx context.Context, script string) (string, error) {
	return this.client.ScriptLoad(ctx, script).Result()
}

// scripting end

// zset start

func (this *Connection) ZAddCtx(ctx context.Context, key string, members ...*contracts.Z) (int64, error) {
	goredisMembers := make([]*goredis.Z, 0)
	for memberKey, value := range members {
		goredisMembers[memberKey] = &goredis.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return this.client.ZAdd(ctx, key, goredisMembers...).Result()
}

func (this *Connection) ZCardCtx(ctx context.Context, key string) (int64, error) {
	return this.client.ZCard(ctx, key).Result()
}

func (this *Connection) ZCountCtx(ctx context.Context, key, min, max string) (int64, error) {
	return this.client.ZCount(ctx, key, min, max).Result()
}

func (this *Connection) ZIncrByCtx(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return this.client.ZIncrBy(ctx, key, increment, member).Result()
}

func (this *Connection) ZInterStoreCtx(ctx context.Context, destination string, store *contracts.ZStore) (int64, error) {
	return this.client.ZInterStore(ctx, destination, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (this *Connection) ZLexCountCtx(ctx context.Context, key, min, max string) (int64, error) {
	return this.client.ZLexCount(ctx, key, min, max).Result()
}

func (this *Connection) ZPopMaxCtx(ctx context.Context, key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := this.client.ZPopMax(ctx, key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (this *Connection) ZPopMinCtx(ctx context.Context, key string, count ...int64) ([]contracts.Z, error) {
	results := make([]contracts.Z, 0)
	goredisResults, err := this.client.ZPopMin(ctx, key, count...).Result()
	for resultKey, value := range goredisResults {
		results[resultKey] = contracts.Z{
			Score:  value.Score,
			Member: value.Member,
		}
	}
	return results, err
}

func (this *Connection) ZRangeCtx(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return this.client.ZRange(ctx, key, start, stop).Result()
}

func (this *Connection) ZRangeByLexCtx(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRangeByLex(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRevRangeByLexCtx(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRevRangeByLex(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRangeByScoreCtx(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRankCtx(ctx context.Context, key, member string) (int64, error) {
	return this.client.ZRank(ctx, key, member).Result()
}

func (this *Connection) ZRemCtx(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return this.client.ZRem(ctx, key, members...).Result()
}

func (this *Connection) ZRemRangeByLexCtx(ctx context.Context, key, min, max string) (int64, error) {
	return this.client.ZRemRangeByLex(ctx, key, min, max).Result()
}

func (this *Connection) ZRemRangeByRankCtx(ctx context.Context, key string, start, stop int64) (int64, error) {
	return this.client.ZRemRangeByRank(ctx, key, start, stop).Result()
}

func (this *Connection) ZRemRangeByScoreCtx(ctx context.Context, key, min, max string) (int64, error) {
	return this.client.ZRemRangeByScore(ctx, key, min, max).Result()
}

func (this *Connection) ZRevRangeCtx(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return this.client.ZRevRange(ctx, key, start, stop).Result()
}

func (this *Connection) ZRevRangeByScoreCtx(ctx context.Context, key string, opt *contracts.ZRangeBy) ([]string, error) {
	return this.client.ZRevRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    opt.Min,
		Max:    opt.Max,
		Offset: opt.Offset,
		Count:  opt.Count,
	}).Result()
}

func (this *Connection) ZRevRankCtx(ctx context.Context, key, member string) (int64, error) {
	return this.client.ZRevRank(ctx, key, member).Result()
}

func (this *Connection) ZScoreCtx(ctx context.Context, key, member string) (float64, error) {
	return this.client.ZScore(ctx, key, member).Result()
}

func (this *Connection) ZUnionStoreCtx(ctx context.Context, key string, store *contracts.ZStore) (int64, error) {
	return this.client.ZUnionStore(ctx, key, &goredis.ZStore{
		Keys:      store.Keys,
		Weights:   store.Weights,
		Aggregate: store.Aggregate,
	}).Result()
}

func (this *Connection) ZScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return this.client.ZScan(ctx, key, cursor, match, count).Result()
}

// zset end