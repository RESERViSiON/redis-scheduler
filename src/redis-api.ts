import { Redis } from "ioredis";
import { MaxRetriesPerRequestError } from "ioredis/built/errors";
import { ConfigType } from "./types";
import { BehaviorSubject, Observable } from "rxjs";

export class RedisApi {
    private redis: Redis | null = null;
    private readonly isConnected$ = new BehaviorSubject<boolean>(false);
    public readonly isConnected = this.isConnected$.asObservable();

    constructor(config?: ConfigType) {
        this.redis = new Redis({
            host: config?.redisHost ?? 'localhost',
            port: config?.redisPort ?? 6379,
            retryStrategy: times => Math.min(times * 50, config?.nrOfRetries ? config.nrOfRetries * 50 : 2000),
            db: config?.redisDb ?? 0,
        });

        //this.redis.on('error', x => console.error(x));

        this.redis.on('connect', () => this.isConnected$.next(true));
        this.redis.on('close', () => this.isConnected$.next(false));
        this.redis.on('error', e => {
            if(e instanceof MaxRetriesPerRequestError)
                throw new Error(e.message);
        });
    }

    async peekFromSet(sortedSetKey: string) {
        const result = await this.redis!.zrange(sortedSetKey,0,Date.now(),'BYSCORE','LIMIT',0,1);

        return result?.length ? result[0] : null;
    }

    enqueueToSet(sortedSetKey: string, taskId: string | number, score: number) {
        return this.redis!.zadd(sortedSetKey, score, taskId);
    }

    removeFromSet(sortedSetKey: string, taskId: string | number) {
        return this.redis!.zrem(sortedSetKey, taskId);
    }

    incrCounter(key: string) {
        return this.redis!.incr(key);
    }

    setString(key: string, value: string) {
        return this.redis!.set(key, value, 'GET');
    }
    
    getString(key: string) {
        return this.redis!.get(key);
    }
    
    removeString(key: string) {
        return this.redis!.del(key);
    }
}