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
let carritos_perdidos = [];
const check_ubicacion = async () => {
	const consumer = kafka.consumer({ groupId: "posicion_carritos" });

	await consumer.connect();
	await consumer.subscribe({ topic: "coordenadas" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());
				let coordenada_carrito = "(" + data.cordx + "," + data.cordy + ")";
				coordenada_carritos.set(data.idcarrito, coordenada_carrito);
				fecha_carritos.set(data.idcarrito, Date.now());
				console.log(
					"El carrito ",
					data.idcarrito,
					"Se encuentra en (",
					data.cordx,
					", ",
					data.cordy,
					")"
				);
				//eliminar de carritos perdidos
				if (
					carritos_perdidos.find((element) => element == data.idcarrito) ==
					data.idcarrito
				) {
					carritos_perdidos.remove(data.idcarrito);
				}
				//revisa si la última actualización fue hace mas de 1 minuto
				function check_Date(value, key, map) {
					const ultima_actualización = Date.now() - value;
					if (ultima_actualización > 60000) {
						console.log("El carro", data.idcarrito, "está perdido");
						//agrega a carritos perdidos
						carritos_perdidos.push(data.idcarrito);
					}
				}
				fecha_carritos.forEach(check_Date);
			}
		},
	});
};

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	check_ubicacion();
});
