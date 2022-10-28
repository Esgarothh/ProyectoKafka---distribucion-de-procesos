const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");

const port = process.env.PORT;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
	brokers: [process.env.kafkaHost],
});

const producer = kafka.producer();

app.post("/carrito_profugo", async (req, res) => {
	let mensaje = {
		idcarrito: "carrito01",
		cordx: 3,
		cordy: 4,
	};

	let carrito = {
		idcarrito: mensaje.idcarrito,
		corx: 3,
		cordy: 4,
	};
	console.log("agente extraño.");
	await producer.connect();
	await producer.send({
		topic: "posicion_carritos",
		messages: [{ value: JSON.stringify(carrito) }],
	});
	await producer.disconnect().then(
		res.status(200).json({
			user: req.body.producto,
			pass: req.body.precio,
		})
	);
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
});
