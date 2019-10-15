"use strict";
var __assign = (this && this.__assign) || Object.assign || function(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
        s = arguments[i];
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
            t[p] = s[p];
    }
    return t;
};
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) if (e.indexOf(p[i]) < 0)
            t[p[i]] = s[p[i]];
    return t;
};
Object.defineProperty(exports, "__esModule", { value: true });
var Kafka = require("kafka-node");
var Logger = require("bunyan");
var child_logger_1 = require("./child-logger");
var pubsub_async_iterator_1 = require("./pubsub-async-iterator");
var defaultLogger = Logger.createLogger({
    name: 'pubsub',
    stream: process.stdout,
    level: 'info'
});
var KafkaPubSub = (function () {
    function KafkaPubSub(options) {
        this.options = options;
        this.subscriptionMap = {};
        this.channelSubscriptions = {};
        if (!this.options.skipConsumer) {
            this.consumer = this.createConsumer(this.options.topic);
        }
        this.logger = child_logger_1.createChildLogger(this.options.logger || defaultLogger, 'KafkaPubSub');
    }
    KafkaPubSub.prototype.publish = function (payload) {
        var _this = this;
        var clientOptions = this.options.kafkaOptions || {};
        var client = new Kafka.KafkaClient(__assign({ kafkaHost: this.options.host }, clientOptions));
        var producer = new Kafka.Producer(client);
        producer.on('error', function (err) {
            _this.logger.error(err, 'Producer error in our kafka stream');
        });
        producer.on('ready', function () {
            producer.send([{
                    topic: _this.options.topic,
                    messages: JSON.stringify(payload),
                    partition: _this.options.partition || 0
                }], function (err, data) {
                if (err) {
                    _this.logger.error(err, 'Error while publishing new kafka event');
                }
            });
        });
        return true;
    };
    KafkaPubSub.prototype.subscribe = function (channel, onMessage, options) {
        var index = Object.keys(this.subscriptionMap).length;
        this.subscriptionMap[index] = [channel, onMessage];
        this.channelSubscriptions[channel] = (this.channelSubscriptions[channel] || []).concat([
            index
        ]);
        return Promise.resolve(index);
    };
    KafkaPubSub.prototype.unsubscribe = function (index) {
        var channel = this.subscriptionMap[index][0];
        this.channelSubscriptions[channel] = this.channelSubscriptions[channel].filter(function (subId) { return subId !== index; });
    };
    KafkaPubSub.prototype.asyncIterator = function (triggers) {
        return new pubsub_async_iterator_1.PubSubAsyncIterator(this, triggers);
    };
    KafkaPubSub.prototype.onMessage = function (channel, message) {
        var subscriptions = this.channelSubscriptions[channel];
        if (!subscriptions) {
            return;
        }
        for (var _i = 0, subscriptions_1 = subscriptions; _i < subscriptions_1.length; _i++) {
            var subId = subscriptions_1[_i];
            var _a = this.subscriptionMap[subId], cnl = _a[0], listener = _a[1];
            listener(message);
        }
    };
    KafkaPubSub.prototype.createConsumer = function (topic) {
        var _this = this;
        var clientOptions = this.options.kafkaOptions || {};
        var client = new Kafka.KafkaClient(__assign({ kafkaHost: this.options.host }, clientOptions));
        var consumer = new Kafka.Consumer(client, [
            {
                topic: topic,
                partition: this.options.partition || 0,
            }
        ], this.options.kafkaConsumerOptions || {});
        consumer.on('message', function (message) {
            var strMessage = typeof message.value === 'string' ? message.value : message.value.toString();
            var parsedMessage;
            try {
                parsedMessage = JSON.parse(strMessage);
            }
            catch (err) {
                _this.logger.error(err, 'Could not parse Kafka message');
                return;
            }
            if (parsedMessage.channel) {
                var channel = parsedMessage.channel, payload = __rest(parsedMessage, ["channel"]);
                _this.onMessage(parsedMessage.channel, payload);
            }
            else {
                _this.onMessage(topic, parsedMessage);
            }
        });
        consumer.on('error', function (err) {
            _this.logger.error(err, 'Consumer error in our kafka stream');
        });
        return consumer;
    };
    return KafkaPubSub;
}());
exports.KafkaPubSub = KafkaPubSub;
//# sourceMappingURL=kafka-pubsub.js.map