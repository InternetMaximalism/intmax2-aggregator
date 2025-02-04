export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
}

export type AggregatorType = "withdrawal" | "claim";
