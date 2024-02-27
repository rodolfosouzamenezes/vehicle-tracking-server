import { Job } from 'bull';
import { Process, Processor } from '@nestjs/bull';
import { Inject } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Processor('kafka-producer')
export class RouteKafkaProducerJob {
  constructor(
    @Inject('KAFKA_SERVICE')
    private kafikaService: ClientKafka,
  ) {}

  @Process()
  async handle(job: Job<any>) {
    await this.kafikaService.emit('route', job.data);

    return {};
  }
}
