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

const auth = async () => {
	const consumer = kafka.consumer({ groupId: "auth", fromBeginning: true });
	await consumer.connect();
	await consumer.subscribe({ topic: "auth" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());
				if (black_list.includes(data.user)) {
					console.log(
						new Date(data.time).toLocaleDateString("es-CL"),
						new Date(data.time).toLocaleTimeString("es-CL"),
						data.user,
						"esta baneado."
					);
				} else if (white_list.has(data.user)) {
					last5 = white_list.get(data.user);
					if (last5.length > 4) last5.shift();
					last5.push(data);
					if (last5.length == 5) {
						diff = last5[4].time - last5[0].time;
						if (diff < 60000) {
							black_list.push(data.user);
							white_list.delete(data.user);
							console.log(
								new Date(data.time).toLocaleDateString("es-CL"),
								new Date(data.time).toLocaleTimeString("es-CL"),
								data.user,
								"fue baneado."
							);
						}
					} else {
						console.log(
							new Date(data.time).toLocaleDateString("es-CL"),
							new Date(data.time).toLocaleTimeString("es-CL"),
							data.user,
							"ingreso."
						);
					}
				} else {
					console.log(
						new Date(data.time).toLocaleDateString("es-CL"),
						new Date(data.time).toLocaleTimeString("es-CL"),
						data.user,
						"ingreso."
					);
					last5 = [data];
					white_list.set(data.user, last5);
				}
			}
		},
	});
};

app.get("/blocked", async (req, res) => {
	res.status(200).json({ "users-blocked": black_list });
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	auth();
});
