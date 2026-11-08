/**
 * queue-lab worker (Node.js, TypeScript): the entry point. One image, two roles, chosen by the ROLE variable.
 */
import * as consumer from './consumer.js';
import * as producer from './producer.js';
import * as settings from './settings.js';

console.log(`${settings.ROLE} ${settings.WID} starting`);
(settings.ROLE === 'consumer' ? consumer.run() : producer.run()).catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
