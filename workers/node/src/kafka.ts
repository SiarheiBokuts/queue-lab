/** The one Kafka client instance of this worker (Confluent's client, KafkaJS-compatible API). */
import { KafkaJS } from '@confluentinc/kafka-javascript';
import * as settings from './settings.js';

export const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers: [settings.BOOT], clientId: settings.WID, logLevel: KafkaJS.logLevel.ERROR },
});