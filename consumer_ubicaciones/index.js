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

var white_list = new Map();
var black_list = [];

let coordenada_carritos = new Map();
let fecha_carritos = new Map();

let carritos_activos = [];
let carritos_perdidos = [];
let carritos_profugos = [];

const update = (array, i, idcarrito, cordx, cordy, tiempo) => {
	array[i].cordx = cordx;
	array[i].cordy = cordy;
	array[i].tiempo = tiempo;
};

const agregar_carrito = (idcarrito, cordx, cordy, tiempo) => {
	i = carritos_profugos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) {
		carritos_profugos.splice(i, 1);
	}
	i = carritos_perdidos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) {
		carritos_perdidos.splice(i, 1);
	}
	i = carritos_activos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) update(carritos_activos, i, idcarrito, cordx, cordy, tiempo);
	else {
		let carrito = {};
		carrito.idcarrito = idcarrito;
		carrito.cordx = cordx;
		carrito.cordy = cordy;
		carrito.tiempo = tiempo;
		carritos_activos.push(carrito);
	}
};

const check_estado = () => {
	carritos_activos.forEach((carrito, index) => {
		let ultima_actualizacion = Date.now() - carrito.tiempo;
		if (ultima_actualizacion > 10000) {
			console.log("El carro", carrito.idcarrito, "está perdido");
			//agrega a carritos perdidos
			carritos_perdidos.push(carritos_activos.splice(index, 1)[0]);
		}
	});
};

let set_carrito_profugo = (idcarrito, cordx, cordy) => {
	i = carritos_perdidos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) {
		carritos_perdidos.splice(i, 1);
	}
	i = carritos_activos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) {
		carritos_activos.splice(i, 1);
	}
	i = carritos_profugos.findIndex((i) => i.idcarrito === idcarrito);
	if (i >= 0) update(carritos_profugos, i, idcarrito, cordx, cordy);
	else {
		let carrito = {};
		carrito.idcarrito = idcarrito;
		carrito.cordx = cordx;
		carrito.cordy = cordy;
		carritos_profugos.push(carrito);
	}
};

let minuto = 5000;

const check_ubicacion = async () => {
	let inicio = Date.now();
	const consumer = kafka.consumer({ groupId: "posicion_carritos" });
	await consumer.connect();
	await consumer.subscribe({ topic: "coordenadas" });
	let run = async (consumer) => {
		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				if (message.value) {
					if (partition === 0) {
						var data = JSON.parse(message.value.toString());
						agregar_carrito(data.idcarrito, data.cordx, data.cordy, Date.now());
						console.log(
							"El carrito ",
							data.idcarrito,
							"Se encuentra en (",
							data.cordx,
							", ",
							data.cordy,
							")",
							"PARTICIOOON=",
							partition
						);
					}
					if (partition === 1) {
						var data = JSON.parse(message.value.toString());
						set_carrito_profugo(data.idcarrito, data.cordx, data.cordy);
					}

					//eliminar de carritos perdidos
				}
			},
		});
	};
	function both() {
		check_estado();
		run(consumer);
	}
	setInterval(both, 5000);
};

app.get("/coordenadas_carritos", async (req, res) => {
	console.log(
		"carritos_Activos=",
		carritos_activos,
		"carritos_perdidos",
		carritos_perdidos,
		"carritos_profugos",
		carritos_profugos
	);
	res.status(200).json({
		carritos_activos: carritos_activos,
		carritos_perdidos: carritos_perdidos,
		carritos_profugos: carritos_profugos,
	});
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	check_ubicacion();
});
