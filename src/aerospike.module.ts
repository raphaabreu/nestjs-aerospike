import { DynamicModule } from '@nestjs/common';
import { AerospikeService } from './aerospike.service';
import { AerospikeConfig } from './aerospike.config';

export class AerospikeModule {
  static forRoot(config: AerospikeConfig): DynamicModule {
    if (!config.hosts) {
      config.hosts = process.env.AEROSPIKE || 'localhost:3000';
    }
    if (!config.retryCount) {
      config.retryCount = 3;
    }
    if (!config.retryBackoff) {
      config.retryBackoff = 20;
    }

    return {
      module: AerospikeModule,
      providers: [
        {
          provide: AerospikeService,
          useFactory: async () => {
            const serv = new AerospikeService(config);

            await serv.connect();

            return serv;
          },
        },
      ],
      exports: [AerospikeService],
    };
  }
}
