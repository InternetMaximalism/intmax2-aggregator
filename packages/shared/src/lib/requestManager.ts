import Redis from "ioredis";
import type { AggregatorType, WithdrawalGroup } from "../types";
import { RedisClient } from "./redis";

export class RequestManager {
  private static instance: RequestManager;
  private redis: Redis;
  private readonly keyPrefix: string;
  private readonly groupSetKey: string;
  private readonly expiration = 30 * 60; // NOTE: expiration 30 minutes

  constructor(requestName: AggregatorType) {
    this.redis = RedisClient.getInstance().getClient()!;
    this.keyPrefix = this.getKeyPrefixByType(requestName);
    this.groupSetKey = `${requestName}:groups`;
  }

  private getKeyPrefixByType(requestName: AggregatorType): string {
    const prefixMap: Record<AggregatorType, string> = {
      withdrawal: "withdrawal-aggregator:",
      claim: "claim-aggregator:",
    };
    return prefixMap[requestName];
  }

  public static getInstance(requestName: AggregatorType): RequestManager {
    if (
      !RequestManager.instance ||
      RequestManager.instance.keyPrefix !== RequestManager.instance.getKeyPrefixByType(requestName)
    ) {
      RequestManager.instance = new RequestManager(requestName);
    }
    return RequestManager.instance;
  }

  private getKey(id: string): string {
    return `${this.keyPrefix}${id}`;
  }

  async getGroup(id: string): Promise<WithdrawalGroup | null> {
    const data = await this.redis.get(this.getKey(id));
    return data ? JSON.parse(data) : null;
  }

  async addGroup(group: WithdrawalGroup): Promise<string> {
    const id = crypto.randomUUID();
    const key = this.getKey(id);
    const timestamp = Date.now();
    const pipeline = this.redis.pipeline();

    pipeline.set(key, JSON.stringify(group));
    pipeline.expire(key, this.expiration);
    pipeline.zadd(this.groupSetKey, timestamp, id);

    await pipeline.exec();

    return id;
  }

  async updateGroup(id: string, updates: Partial<WithdrawalGroup>) {
    const key = this.getKey(id);
    const current = await this.getGroup(id);

    if (!current) {
      return false;
    }

    const updated: WithdrawalGroup = {
      ...current,
      ...updates,
      updatedAt: new Date(),
    };

    await this.redis.set(key, JSON.stringify(updated));

    return true;
  }

  async deleteGroup(id: string): Promise<void> {
    const key = this.getKey(id);
    const pipeline = this.redis.pipeline();

    pipeline.del(key);
    pipeline.zrem(this.groupSetKey, id);

    await pipeline.exec();
  }

  async getAllGroups(): Promise<WithdrawalGroup[]> {
    const ids = await this.redis.zrange(this.groupSetKey, 0, -1);
    const groups = await Promise.all(ids.map((id) => this.getGroup(id)));
    return groups.filter((group): group is WithdrawalGroup => group !== null);
  }

  async getAllProcessedUUIDs(): Promise<string[]> {
    const groups = await this.getAllGroups();
    return groups.flatMap((group) => group.requestingWithdrawals.map(({ uuid }) => uuid));
  }
}
