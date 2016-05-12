const expect    = require('chai').expect;
const _         = require('lodash');
const RabbitBus = require('../');

const connectionOptions = process.env.RABBITMQ_URL || 'amqp://localhost';

describe('Test the basic features of the RabbitBus', () => {

  it('Should connect, publish to a channel, and receive the message to a subscriber', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher  = connections[0];
      const subscriber = connections[1];

      subscriber.subscribe('message', (payload) => {
        expect(payload.message).to.equal(message);
        Promise.all([publisher.end(), subscriber.end()])
        .then(() => done());
      })
      .then(() => {
        publisher.publish('message', { message });
      });
    })
    .catch(err => {
      console.log('err', err);
    });
  });

  it('Should connect, publish an error to a channel, and the subscriber should get the error message', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message    = { error: new Error('Test error message') };
      const publisher  = connections[0];
      const subscriber = connections[1];

      subscriber.subscribe('message', (payload) => {
        expect(payload.message.error).to.equal(message.error.message);
        Promise.all([publisher.end(), subscriber.end()])
          .then(() => done());
      })
      .then(() => {
        publisher.publish('message', { message });
      });
    })
    .catch(err => {
      console.log('err', err);
    });
  });

  it('Should connect, publish to a channel, and only one of the subscribers should get the message', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher  = connections[0];
      const subscriber = connections[1];

      subscriber.subscribe('message', (payload) => {
        expect(payload.message).to.equal(message);
        Promise.all([publisher.end(), subscriber.end()])
        .then(() => done());
      })
      .then(() => {
        return subscriber.subscribe('not-the-message', (payload) => {
            done('Should not get the published message!');
          });
      })
      .then(() => {
        publisher.publish('message', { message });
      });
    })
    .catch(done);
  });

  it('Should connect, publish to a channel, and have two subscribers get the message', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher   = connections[0];
      const subscriber1 = connections[1];
      const subscriber2 = connections[2];

      const allDone = _.after(2, ()=>  {
        Promise.all([publisher.end(), subscriber1.end(), subscriber2.end()])
          .then(() => done());
      });

      Promise.all([
        subscriber1.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        }),
        subscriber2.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        })
      ])
      .then(() => {
        publisher.publish('message', { message });
      });
    })
    .catch(done);
  });

  it('Should use one publisher instance and one subscriber instance two pub/sub to two different channels', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher  = connections[0];
      const subscriber = connections[1];

      const allDone = _.after(2, ()=>  {
        Promise.all([publisher.end(), subscriber.end()])
          .then(() => done());
      });

      Promise.all([
        subscriber.subscribe('firstMessage', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        }),
        subscriber.subscribe('secondMessage', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        })
      ])
      .then(() => {
        publisher.publish('firstMessage', { message });
        publisher.publish('secondMessage', { message });
      });
    })
    .catch(done);
  });

  it('Should use one publisher instance and two subscribers listening to the same channel', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher   = connections[0];
      const subscriber1 = connections[1];
      const subscriber2 = connections[2];

      const allDone = _.after(2, ()=>  {
        Promise.all([publisher.end(), subscriber1.end(), subscriber2.end()])
          .then(() => done());
      });

      Promise.all([
        subscriber1.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        }),
        subscriber2.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        })
      ])
      .then(() => {
        publisher.publish('message', { message });
      });
    })
    .catch(done);
  });

  it('Should use two publisher instances and two subscribers listening to the same channel', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message = 'Test message';
      const publisher1   = connections[0];
      const publisher2   = connections[1];
      const subscriber1 = connections[2];
      const subscriber2 = connections[3];

      const allDone = _.after(4, ()=>  {
        Promise.all([publisher1.end(), publisher2.end(), subscriber1.end(), subscriber2.end()])
          .then(() => done());
      });

      Promise.all([
        subscriber1.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        }),
        subscriber2.subscribe('message', (payload) => {
          expect(payload.message).to.equal(message);
          allDone();
        })
      ])
      .then(() => {
        publisher1.publish('message', { message });
        publisher2.publish('message', { message });
      });
    })
    .catch(done);
  });

  it('Should publish 1000 messages', (done) => {
    Promise.all([
      new RabbitBus(connectionOptions),
      new RabbitBus(connectionOptions)
    ])
    .then((connections) => {
      const message       = 'Test message';
      const publisher     = connections[0];
      const subscriber    = connections[1];
      const messageAmount = 1000;

      const allDone = _.after(messageAmount, ()=>  {
        Promise.all([publisher.end(), subscriber.end()])
          .then(() => done());
      });

      subscriber.subscribe('lotsOfMessages', (payload) => {
        expect(payload.message).to.equal(message);
        allDone();
      })
      .then(() => {
        _.times(messageAmount, () => {
          publisher.publish('lotsOfMessages', { message });
        });
      });
    })
    .catch(done);
  });

});
