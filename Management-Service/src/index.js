const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const router = express.Router();
require('dotenv').config();
const {Sequelize, Model, DataTypes} = require('sequelize');
const cors = require('cors');
const {publishToQueue} = require('./Services/MQService');
const Hashids = require('hashids/cjs')
const hashids = new Hashids('Url shortener', 6)

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use(cors());
app.options('*', cors());

const DATABASE_NAME = process.env.DATABASE_NAME;
const DATABASE_USER = process.env.DATABASE_USER;
const DATABASE_PASSWORD = process.env.DATABASE_PASSWORD;

const sequelize = new Sequelize(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, {
    host: process.env.DATABASE_HOST,
    dialect: 'mysql'
});

sequelize.authenticate().then(() => {
    console.log('Connection esstablished');
});

const APP_URL = process.env.REDIRECTION_APP_URL;
const PORT = process.env.APP_PORT;
const HOST = '0.0.0.0';

const Url = sequelize.define('Url', {
    realUrl: {
        type: DataTypes.STRING
    }
}, {
    tableName: 'urls'
});

app.get('/', function (req, res) {
    return res.json('Hello World!')
});

app.post('/', async (req, res) => {
    const url = req.body.realUrl
    const createdUrl = await createUrl(url)
    return res.json(createdUrl)
})

app.delete('/:id', async (req, res) => {
    await deleteUrl(Number(req.params.id))
    return res.json('Success');
})

async function createUrl(realUrl) {
    const msgQueueUrl = await Url.build({realUrl: realUrl}).save();
    const hash = hashids.encode(msgQueueUrl.id)
    const shortUrl = `${APP_URL}/${hash}`
    msgQueueUrl.setDataValue('shortUrlHash', hash);
    await publishToQueue(msgQueueUrl, 'urls.create');
    msgQueueUrl.setDataValue('shortUrl', shortUrl);
    return msgQueueUrl;
}

async function deleteUrl(urlId) {
    await Url.destroy({
        where: {
            id: urlId
        }
    })
    await publishToQueue(hashids.encode(urlId), 'urls.delete')
}

app.listen(PORT, HOST);
