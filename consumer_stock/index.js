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

let pedidos = new Map();
let carritos = new Map();

const nueva_relacion = (nombre, cantidad) => {
	let bool = false;
	pedidos.forEach((valor) => {
		if (valor.nombre === nombre) bool = true;
	});
	if (!bool) {
		agregar_maestro(nombre, maestros_sopaipilleros);
	}
	if (bool) {
		console.log("maestro ya registrado");
	}
};

let npedido = 0;

let lista_pedidos = [];

const check_stock = () => {
	let pedido = {};
	carritos.forEach((value, key) => {
		if (value < 20) {
			pedidos.set(key, value);
			carritos.delete(key);
			console.log(
				"El carrito ",
				key,
				"se ha registrado para reposición de sopaipillas"
			);
		}
	});
	if (pedidos.size === 5) {
		console.log("Proxima reposicion, pedido N°:", npedido);
		pedidos.forEach((value, key) => {
			pedido.nombrecarrito = key;
			pedido.stock_restante = value;
			pedido.npedido = npedido;
			lista_pedidos.push(pedido);

			console.log("Carrito", key, " ultimo stock:", value);
		});
		npedido += 1;
		pedidos.clear();
	}
};

const receive_stock = async () => {
	const consumer = kafka.consumer({ groupId: "stocks" });

	await consumer.connect();
	await consumer.subscribe({ topic: "stock" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());

				// var data = message.value
				console.log(
					"El carrito:",
					data.idcarrito,
					"tiene ",
					"Stock restaste:",
					data.stock_restante
				);
				carritos.set(data.idcarrito, parseInt(data.stock_restante));
				check_stock();
			}
		},
	});
};

app.get("/ver_reposiciones", async (req, res) => {
	res.status(200).json({ "ultimas reposiciones ": lista_pedidos });
	console.log("Se necesita reposicion para: ", lista_pedidos);
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	receive_stock();
});
