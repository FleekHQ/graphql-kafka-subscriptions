import * as Kafka from 'kafka-node';
import { PubSubEngine } from 'graphql-subscriptions'
import * as Logger from 'bunyan';
import { createChildLogger } from './child-logger';
import { PubSubAsyncIterator } from './pubsub-async-iterator'

export interface IKafkaOptions {
  topic: string
  host: string
  partition?: number
  logger?: Logger,
  skipConsumer?: boolean,
  kafkaOptions?: any
  kafkaConsumerOptions?: any
  kafkaProducerOptions?: any
}

export interface IKafkaProducer {
  write: (input: Buffer) => any
}

export interface IKafkaTopic {
  readStream: any
  writeStream: any
}

const defaultLogger = Logger.createLogger({
  name: 'pubsub',
  stream: process.stdout,
  level: 'info'
})

export class KafkaPubSub implements PubSubEngine {
  protected producer: any
  protected consumer: any
  protected options: any
  protected subscriptionMap: { [subId: number]: [string, Function] }
  protected channelSubscriptions: { [channel: string]: Array<number> }
  protected logger: Logger

  constructor(options: IKafkaOptions) {
    this.options = options
    this.subscriptionMap = {}
    this.channelSubscriptions = {}

    // bypass consumer for instances that will only
    // produce
    if(!this.options.skipConsumer){
      this.consumer = this.createConsumer(this.options.topic)
    }

    this.logger = createChildLogger(
      this.options.logger || defaultLogger, 'KafkaPubSub')
  }

  public publish(payload) {
    // only create producer if we actually publish something
    const clientOptions = this.options.kafkaOptions || {}
    const client = new Kafka.KafkaClient({ kafkaHost: this.options.host, ...clientOptions })
    const producer = new Kafka.Producer(client)
    producer.on('error', (err) => {
      this.logger.error(err, 'Producer error in our kafka stream')
    })
    producer.on('ready', () => {
      producer.send(
        [{
          topic: this.options.topic,
          messages: JSON.stringify(payload),
          partition: this.options.partition || 0
        }],
        (err, data) => {
          if (err) {
            this.logger.error(err, 'Error while publishing new kafka event')
          }
        }
      )
    })
    return true;
  }

  public subscribe(
    channel: string,
    onMessage: Function,
    options?: Object
): Promise<number> {
    const index = Object.keys(this.subscriptionMap).length
    this.subscriptionMap[index] = [channel, onMessage]
    this.channelSubscriptions[channel] = [
      ...(this.channelSubscriptions[channel] || []), index
    ]
    return Promise.resolve(index)
  }

  public unsubscribe(index: number) {
    const [channel] = this.subscriptionMap[index]
    this.channelSubscriptions[channel] = this.channelSubscriptions[channel].filter(subId => subId !== index)
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers)
  }

  private onMessage(channel: string, message) {
    const subscriptions = this.channelSubscriptions[channel]
    if (!subscriptions) { return } // no subscribers, don't publish msg
    for (const subId of subscriptions) {
      const [cnl, listener] = this.subscriptionMap[subId]
      listener(message)
    }
  }

  private createConsumer(topic: string) {
    const clientOptions = this.options.kafkaOptions || {};
    const client = new Kafka.KafkaClient({ kafkaHost: this.options.host, ...clientOptions })
    const consumer = new Kafka.Consumer(
      client,
      [
        {
          topic,
          partition: this.options.partition || 0,
        }
      ],
      this.options.kafkaConsumerOptions || {},
    )

    consumer.on('message', (message) => {
      const strMessage = typeof message.value === 'string' ? message.value : message.value.toString();
      let parsedMessage;
      try{
        parsedMessage = JSON.parse(strMessage);
      } catch (err) {
        this.logger.error(err, 'Could not parse Kafka message');
        return;
      }

      // Using channel abstraction
      if (parsedMessage.channel) {
        const { channel, ...payload } = parsedMessage
        this.onMessage(parsedMessage.channel, payload)

      // No channel abstraction, publish over the whole topic
      } else {
        this.onMessage(topic, parsedMessage)
      }
    })

    consumer.on('error', (err) => {
      this.logger.error(err, 'Consumer error in our kafka stream')
    })
    return consumer
  }
}
