import { Injectable } from '@nestjs/common';
import * as Aerospike from 'aerospike';
import Semaphore from 'semaphore-async-await';
import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { AerospikeErrors } from './aerospike-errors.enum';
import { AerospikeConfig } from './aerospike.config';

@Injectable()
export class AerospikeService {
  private readonly logger = new StructuredLogger(AerospikeService.name);
  private readonly semaphore: Semaphore;

  private _client: any;

  constructor(private config: AerospikeConfig) {
    this.semaphore = new Semaphore(Math.max((config.maxConnsPerNode || 2) - 1, config.minConnsPerNode || 1));
  }

  async connect(): Promise<boolean> {
    if (this._client) {
      return true;
    }

    try {
      this._client = await Aerospike.connect(this.config);
      this.logger.log('Aerospike connected');
    } catch (err) {
      this.logger.warn('Error while connecting to aerospike server', err);
      return false;
    }

    this._client.on('event', (event: any) => {
      this.logger.debug(`Aerospike event: ${JSON.stringify(event)}, IsConnected: ${this._client.isConnected()}`);
    });

    this._client.on('disconnected', () => {
      this.logger.warn('Disconnected from Aerospike');
    });

    return true;
  }

  async execute<T>(func: (client: any) => T | PromiseLike<T>, retryCount = -1, retryBackoff = -1): Promise<T> {
    if (retryCount < 0) {
      retryCount = this.config.retryCount;
    }
    if (retryBackoff < 0) {
      retryBackoff = this.config.retryBackoff;
    }

    let retry = 0;

    const retryable: () => Promise<T> = async () => {
      try {
        return await this.semaphore.execute(() => func(this._client));
      } catch (err) {
        // client time out
        if (err.code === AerospikeErrors.ERR_TIMEOUT && retry < retryCount) {
          retry++;
          const backoff = Math.pow(retryBackoff, retry);
          this.logger.warn(`Client timed out, trying again in ${backoff}ms (retry ${retry} of ${retryCount})`);
          await new Promise((r) => setTimeout(r, backoff));
          return retryable();
        }

        throw err;
      }
    };

    return retryable();
  }

  get client(): any {
    return this._client;
  }
}
