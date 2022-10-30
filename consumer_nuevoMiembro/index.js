const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");
const fs = require("fs");
const port = process.env.PORT;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
	brokers: [process.env.kafkaHost],
});

let miembros = [];
let miembrosaceptados = [];

const agregar_miembro = (object) => {
	let base = {};
	base.nombre = object.nombre;
	base.apellido = [];
	base.rut = object.rut;
	base.correo = object.correo;
	base.patentecarrito = object.patentecarrito;
	base.idcarrito = object.idcarrito;
	base.premium = object.premium;
	miembros.push(base);
};

const nuevo_miembro = (object) => {
	let bool = false;
	miembros.forEach((valor) => {
		if (valor.nombre === object.nombre) bool = true;
	});
	if (!bool) agregar_miembro(object);
	if (bool) console.log("maestro ya registrado");
};

const registrar = async () => {
	const consumer = kafka.consumer({ groupId: "miembros" });

	await consumer.connect();
	await consumer.subscribe({ topic: "nuevosmiembros" });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			if (message.value) {
				var data = JSON.parse(message.value.toString());
				console.log("data:", data);
				// var data = message.value
				nuevo_miembro(data);
			}
		},
	});
};

const aceptar_caso = (rut) => {
	let index = miembros.findIndex((i) => i.rut === rut);
	if (index >= 0) {
		miembrosaceptados.push(miembros.splice(index, 1)[0]);
		fs.writeFile(
			"miembrosaceptados.txt",
			JSON.stringify(miembrosaceptados),
			(err) => {
				// throws an error, you could also catch it here
				if (err) throw err;

				// success case, the file was saved
				console.log("saved");
			}
		);
	}
};

app.post("/aceptarcaso", async (req, res) => {
	let rut = req.body.rut;
	aceptar_caso(rut);
	res.status(200).json({ miembrospendientes: miembros });
});

app.get("/vermiembros", async (req, res) => {
	console.log(miembros);
	res.status(200).json({
		miembrospendientes: miembros,
		miembrosaceptados: miembrosaceptados,
	});
});

app.listen(port, () => {
	console.log(`Listening on port ${port}`);
	registrar();
});
