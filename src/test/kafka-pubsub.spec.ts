import { KafkaPubSub } from '../index'

const mockWrite = jest.fn((msg) => msg)
const mockProducer = jest.fn(() => {
  console.log('got here wtf');
  return {
  write: mockWrite
  }
})
const mockConsumer = jest.fn(() => {})
const topic = 'test-topic'
const host = 'localhost:9092'
const pubsub = new KafkaPubSub({
  topic,
  host,
})

describe('KafkaPubSub', () => {
  it('should create producer/consumers correctly', () => {
    const onMessage = jest.fn()
    const testChannel = 'testChannel'
    expect(mockProducer).toBeCalledWith(topic)
    expect(mockConsumer).toBeCalledWith(topic)
  })
  it('should subscribe and publish messages correctly', async () => {
    const channel = 'test-channel'
    const onMessage = jest.fn()
    const payload = {
      channel,
      id: 'test',
    }
    const subscription = await pubsub.subscribe(channel, onMessage)
    pubsub.publish(payload)
    expect(mockWrite).toBeCalled()
    expect(mockWrite).toBeCalledWith(new Buffer(JSON.stringify(payload)))
  })
})

describe('KafkaPubSub test consumer skip flag', () => {
  it('should not create consumer if flag is set', () => {
    const pubsub = new KafkaPubSub({
      topic,
      host,
      skipConsumer: true,
    })
    expect(pubsub).toBeDefined()
    expect(pubsub['consumer']).toBeUndefined()
  })
  it('should create consumer if flag is set', () => {
    const pubsub = new KafkaPubSub({
      topic,
      host,
    })
    expect(pubsub).toBeDefined()
    expect(pubsub['consumer']).toBeDefined()
  })
})