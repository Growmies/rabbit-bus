const _    = require('lodash');
const amqp = require('amqplib');
const util = require('./util');

const eventBusChannel = 'event-bus';

function RabbitBus(connectionOptions) {
  this.subscriberChannels = {};
  this.publisherChannels  = {};
  this.channels           = [];

  this.publish = (channelName, payload) => {
    return new Promise((resolve, reject) => {
      this.getPublisherChannel(channelName)
        .then(channel => {
          channel.publish(eventBusChannel, channelName, new Buffer(_.isPlainObject(payload) ? JSON.stringify(payload, util.replaceErrors) :  payload));
          resolve();
        })
        .catch(err => {
          reject(err);
        });
    });
  };

  this.subscribe = (channelName, listener) => {
    return new Promise((resolve, reject) => {
      this.getSubscriberChannel(channelName)
        .spread((channel, queue) => {
          resolve();
          channel.consume(queue, message => {
            listener(_.attempt(JSON.parse, message.content.toString()));
          }, { noAck: true });
        })
        .catch(err => {
          reject(err);
        });
    });
  };

  this.end = () => {
    return Promise.all(
      _.flatten([
        _.map(this.subscriberChannels, (channel) => {
          return channel.spread((c, q) => c.unbindExchange(eventBusChannel, 'direct', ''));
        }),
        _.map(this.publisherChannels, (channel) => {
          return channel.then(c => c.unbindExchange(eventBusChannel, 'direct', ''));
        })
      ]))
    .then(() => {
      return this.connection.close();
    });
  };

  this.getPublisherChannel = channelName => {
    if (this.publisherChannels[channelName]) {
      return this.publisherChannels[channelName];
    }

    this.publisherChannels[channelName] = this.connection.createChannel()
      .then(channel => {
        this.channels.push(channelName);
        channel.assertExchange(eventBusChannel, 'direct', { durable: false });
        return channel;
      });

    return this.publisherChannels[channelName];
  };

  this.getSubscriberChannel = channelName => {
    if (this.subscriberChannels[channelName]) {
      return this.subscriberChannels[channelName];
    }
    this.subscriberChannels[channelName] = this.connection.createChannel()
      .then(channel => {
        this.channels.push(channelName);
        channel.assertExchange(eventBusChannel, 'direct', { durable: false });
        return channel.assertQueue('', { exclusive: true, durable: false })
          .then(q => {
            channel.bindQueue(q.queue, eventBusChannel, channelName);
            return [channel, q.queue];
          });
      });

    return this.subscriberChannels[channelName];
  };

  return new Promise((resolve, reject) => {
    if (_.isString(connectionOptions)) {
      return amqp.connect(connectionOptions)
        .then((connection) => {
          this.connection = connection;
          resolve(this);
        })
        .catch(err => {
          reject(err);
        });
    } else {
      this.connection = connectionOptions; // Is a connection object
      resolve(this);
    }
  });
}

module.exports = RabbitBus;

