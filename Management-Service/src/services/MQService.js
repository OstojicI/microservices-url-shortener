const amqp = require('amqplib')

const QUEUE = 'shortUrl'
const rabbitSettings = {
    protocol: 'amqp',
    hostname: process.env.RABBITMQ_HOST,
    port: process.env.RABBITMQ_PORT,
    username: 'guest',
    password: 'guest',
    virtualHost: '/'
}

async function connect() {
    const conn = await amqp.connect(rabbitSettings);
    const channel = await conn.createChannel();
    await channel.assertQueue(QUEUE)
    return channel;
}

module.exports.publishToQueue = async (data, type) => {
    const ch = await connect();
    ch.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), {type});
}
