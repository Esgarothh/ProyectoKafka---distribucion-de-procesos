const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");
const { removeAllListeners } = require("nodemon");

const port = process.env.PORT;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
	brokers: [process.env.kafkaHost],
});

const producer = kafka.producer();

app.post("/vender", async (req, res) => {
	let tiempo = new Date().getTime();
	let venta = {
		idcarrito: req.body.idcarrito,
		idcliente: req.body.idcliente,
		cantidad: req.body.cantidad,
		tiempo: tiempo,
	};
	let carrito = {
		idcarrito: req.body.idcarrito,
		cordx: req.body.cordx,
		cordy: req.body.cordy,
	};
	let stock = {
		idcarrito: req.body.idcarrito,
		stock_restante: req.body.stock_restante,
	};

	console.log("esta intentando ingresar.");
	await producer.connect();
	await producer.send({
		topic: "venta",
		messages: [{ value: JSON.stringify(venta) }],
	});
	await producer.send({
		topic: "stock",
		messages: [{ value: JSON.stringify(stock) }],
	});
	await producer.send({
		topic: "coordenadas",
		messages: [{ value: JSON.stringify(carrito) }],
		partition: 0,
	});
	await producer.disconnect().then(
		res.status(200).json({
			user: req.body.producto,
			pass: req.body.precio,
		})
	);
});

app.post("/registro", async (req, res) => {
	let miembro = {
		nombre: req.body.nombre,
		apellido: req.body.apellido,
		rut: req.body.rut,
		correo: req.body.correo,
		patentecarrito: req.body.patente,
		idcarrito: req.body.idcarrito,
		premium: req.body.premium,
	};
	if (miembro.premium === "si") {
		await producer.connect();
		await producer.send({
			topic: "nuevosmiembros",
			messages: [{ value: JSON.stringify(miembro) }],
			partition: 1,
		});
	} else if (miembro.premium === "no") {
		await producer.connect();
		await producer.send({
			topic: "nuevosmiembros",
			messages: [{ value: JSON.stringify(miembro) }],
			partition: 0,
		});
	} else {
		res.status(200).json({ error: "error" });
	}
	res.status(200).json({ good: "good" });
});

app.post("/denunciar", async (req, res) => {
	let miembro = {
		idcarrito: req.body.idcarrito,
		cordx: req.body.cordx,
		cordy: req.body.cordy,
	};
	await producer.connect();
	await producer.send({
		topic: "coordenadas",
		messages: [{ value: JSON.stringify(miembro) }],
		partition: 1,
	});
	res.status(200).json({ good: "good" });
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
});
