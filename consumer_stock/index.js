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

const check_stock = async () => {
	const consumer = kafka.consumer({ groupId: "stock" });

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
				if (data.stock_restante < 20) {
					pedidos.set(data.idcarrito, data.stock_restante);
					console.log(
						"El carrito ",
						data.idcarrito,
						"se ha registrado para reposición de sopaipillas"
					);

					if (pedidos.size === 5) {
						console.log("Proxima reposicion, pedido N°:", npedido);
						npedido += 1;
						pedidos.forEach((key, value) => {
							console.log("Carrito", key, " ultimo stock:", value);
						});
						pedidos.clear();
					}
				}
			}
		},
	});
};

app.get("/ver_reposiciones", async (req, res) => {
	res.status(200).json({ "ultimas reposiciones ": pedidos });
	console.log("Se necesita reposicion para: ", pedidos);
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	check_stock();
});
