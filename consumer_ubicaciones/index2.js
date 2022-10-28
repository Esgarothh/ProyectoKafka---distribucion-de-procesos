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

const check_stock = async () => {
	const consumer = kafka.consumer({ groupId: "stock", fromBeginning: true });

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
				if (data.stock_restante < 20){
					console.log("El carrito ", data.idcarrito,
					"necesita reposición de sopaipillas",
					);
					//Se supone que 
					console.log("Reposición automática inicializada");
					
				}
			}
		},
	});
};







app.get("/blocked", async (req, res) => {
	res.status(200).json({ "users-blocked": black_list });
	auth();
});

app.get("/calculardiario", async (req, res) => {
	res.status(200).json({ "calculo diario": black_list });
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	auth();
});
