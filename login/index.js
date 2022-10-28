const express = require("express");
const cors = require("cors");
const { Kafka } = require('kafkajs')

const port = process.env.PORT;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});

const producer = kafka.producer();

app.post("/login", async (req, res) => {
    req.body.time = new Date().getTime();
    console.log(new Date(req.body.time).toLocaleDateString("es-CL"), new Date(req.body.time).toLocaleTimeString("es-CL"), req.body.user, "esta intentando ingresar.");
    await producer.connect();
    await producer.send({
        topic: 'auth',
        messages: [{value: JSON.stringify(req.body)}]
    })
    await producer.disconnect().then(
        res.status(200).json({
            user: req.body.user,
            pass: req.body.pass
        })
    )
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});