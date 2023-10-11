import type { ConfigType } from "./types";
import { RedisApi } from "./redis-api";
import { Observable, filter, switchMap } from "rxjs";

export class RedisSchedulerFactory {
    private api: RedisApi;

    constructor(config?: ConfigType) {
        this.api = new RedisApi(config);
    }

    getInstance<T = string>(topic: string, pollingInterval = 1) {
        return new RedisScheduler<T>(this.api, topic, pollingInterval)
    }
}

class RedisScheduler<T> {
    constructor(private api: RedisApi, private topic: string, private pollingInterval: number) { }

    public listen() {
        return this.api.isConnected.pipe(filter(isConnected => isConnected), switchMap(() => new Observable<T | null>(observer => {
            const findNextTask = async () => {
                let taskId: string | null;
                do {
                    taskId = await this.api.peekFromSet(`sortedTasks_${this.topic}`);
                    if (taskId) {
                        const data = await this.api.getString(`${this.topic}_task:${taskId}`);
                        if (data) {
                            try {
                                observer.next(JSON.parse(data) as T);
                            } catch {
                                observer.error("json parse error");
                            }
                        }
                        this.api.removeString(`${this.topic}_task:${taskId}`);
                        this.api.removeFromSet(`sortedTasks_${this.topic}`, taskId)
                    }
                } while (taskId);

                setTimeout(findNextTask, this.pollingInterval * 1000);
            }

            findNextTask();
        })))
    }

    public async schedule(data: T, timestamp: number) {
        const taskId = await this.api.incrCounter(`scheduleCounter_${this.topic}`);
        await this.api.setString(`${this.topic}_task:${taskId}`, JSON.stringify(data));
        await this.api.enqueueToSet(`sortedTasks_${this.topic}`, taskId, timestamp);
    }
}

export type { RedisScheduler };