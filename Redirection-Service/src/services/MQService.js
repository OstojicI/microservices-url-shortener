const amqp = require('amqplib')

const QUEUE = 'shortUrl';

const rabbitSettings = {
    protocol: 'amqp',
    hostname: process.env.RABBITMQ_HOST,
    port: process.env.RABBITMQ_PORT,
    username: 'guest',
    password: 'guest',
    virtualHost: '/'
}

async function connect() {
    // Try to connect to rabbitmq until successful
    // let conn = null;
    // do {
    //     try {
    //         conn = await amqp.connect(rabbitSettings);
    //     } catch (e) {}
    // } while (conn === null)
    const conn = await amqp.connect(rabbitSettings);
    const channel = await conn.createChannel();
    channel.assertQueue(QUEUE, {
        durable: true
    });
    return channel;
}

module.exports.consumeQueue = async (callback) => {
    const ch = await connect();
    ch.consume(QUEUE, msg => {
        callback(msg);
    }, {noAck: true});
}
