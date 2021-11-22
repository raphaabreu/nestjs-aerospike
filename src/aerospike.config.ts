export type AerospikeConfig = {
  hosts?: string;
  maxConnsPerNode?: number;
  minConnsPerNode?: number;
  retryCount?: number;
  retryBackoff?: number;
  [key: string]: any;
};
