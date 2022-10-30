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

let maestros_sopaipilleros = [];

const agregar_maestro = (idMs, m) => {
	let base = {};
	base.idcarrito = idMs;
	base.clientes = [];
	base.numero_clientes = 0;
	base.cantidad_sopaipillas = 0;
	base.promedio = 0;
	m.push(base);
};

const agregar_venta = (ms, idcliente, cantidad_sopaipillas) => {
	let cliente = {};
	cliente.nombre = idcliente;
	cliente.cantidad = cantidad_sopaipillas;
	indice = maestros_sopaipilleros.findIndex((i) => i.idcarrito === ms);
	let bool = false;
	maestros_sopaipilleros[indice].clientes.forEach((client) => {
		if (client.nombre === idcliente) bool = true;
		//existe
	});
	if (!bool) maestros_sopaipilleros[indice].numero_clientes += 1;
	maestros_sopaipilleros[indice].clientes.push(cliente);
	maestros_sopaipilleros[indice].cantidad_sopaipillas += cantidad_sopaipillas;
	maestros_sopaipilleros[indice].promedio =
		maestros_sopaipilleros[indice].cantidad_sopaipillas /
		maestros_sopaipilleros[indice].numero_clientes;
};
const nuevo_maestro = (nombre) => {
	let bool = false;
	maestros_sopaipilleros.forEach((valor) => {
		if (valor.idms === nombre) bool = true;
	});
	if (!bool) agregar_maestro(nombre, maestros_sopaipilleros);
	if (bool) console.log("maestro ya registrado");
};

const ventas_totales = async () => {
	const consumer = kafka.consumer({ groupId: "ventas" });
	await consumer.connect();
	await consumer.subscribe({ topic: "venta" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());
				console.log(
					"mensaje:",
					message.value,
					"data:",
					data,
					"datacarrito:",
					data.idcarrito
				);
				// var data = message.value
				nuevo_maestro(data.idcarrito);
				agregar_venta(data.idcarrito, data.idcliente, parseInt(data.cantidad));
			}
		},
	});
};

const ventas_totales_premium = async () => {
	const consumer = kafka.consumer({ groupId: "ventas" });
	await consumer.connect();
	await consumer.subscribe({ topic: "venta" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());
				console.log(
					"mensaje:",
					message.value,
					"data:",
					data,
					"datacarrito:",
					data.idcarrito
				);
				// var data = message.value
				nuevo_maestro(data.idcarrito);
				agregar_venta(data.idcarrito, data.idcliente, parseInt(data.cantidad));
			}
		},
	});
};

app.get("/consumir", async (req, res) => {
	ventas_totales();
	console.log(maestros_sopaipilleros);
	res.status(200).json({ maestros: maestros_sopaipilleros });
});

app.get("/calculardiario", async (req, res) => {
	res.status(200).json({ "calculo diario": black_list });
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
});
