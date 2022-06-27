/**
 * 
 */
use test;
db.dropDatabase();

// Comandos 
db.commandHelp("find");
db.commandHelp("count");
db.runCommand( { count: 'persona' } )

//Crear base de datos Agenda
use AgendaBD;

//Insertar empleado 1, José Juan Martínez González
db.empleado.insertOne({numEmpleado:1, nombre:"José Juan", apPaterno:"Martínez", apMaterno:"González"});

//Crear colección empleado con validador de esquema (fallará por ya existir)
db.createCollection("empleado", {
	validator: {
	  $jsonSchema: {
		bsonType: "object",
		required: ["numEmpleado", "nombre", "apPaterno", "apMaterno"]
		}
	}
});
// Borrar colección empleado 
db.empleado.drop();
//Crear colección empleado con validador de esquema 
db.createCollection("empleado", {
	validator: {
	  $jsonSchema: {
		bsonType: "object",
		required: ["numEmpleado", "nombre", "apPaterno", "apMaterno"]
		}
	}
});

// Insertar empleado 1, José Juan Martínez González
db.empleado.insertOne({numEmpleado:1, nombre:"José Juan", apPaterno:"Martínez", apMaterno:"González"});
// Agregar índice ascendente de unicidad sobre la llave primaria de negocio numEmpleado
db.empleado.createIndex( {numEmpleado:1} , {unique:true});

// Insertar empleados 2, 3 y 4
db.empleado.insertMany([
	{numEmpleado:2, nombre:"Martín", apPaterno:"De la Torre", apMaterno:"García"},	
	{numEmpleado:3, nombre:"Claudia", apPaterno:"De la Rosa", apMaterno:"Morientes"},
	{numEmpleado:4, nombre:"Macario", apPaterno:"Ahuizote", apMaterno:"Moreno"}
	]);

// Insertar eventos
db.evento.insertOne({
	numEvento:2, nombre:"Junta de planeción sig. versión portal 2", 
	           objetivo:"Definir el plan preliminar de desarrollo del portal",
	           fechaProgramada: new Date("2019-11-04T13:00:00"),
	           duracionProgramadaMin: 90,
	           participaciones:[{numParticipacion:1, numEmpleado:1, tipoParticipacion:1, rol:"patrocinador"},
	        	   { numParticipacion:2, numEmpleado: 3, tipoParticipacion:2, rol:"product owner"},
	        	   { numParticipacion:3, numEmpleado: 4, tipoParticipacion:2, rol:"scrum master"}
	           ]});
db.evento.insertOne({
	numEvento:3, nombre:"Junta de seguimiento semanal", 
	           objetivo:"Actualizar avances en la semana",
	           fechaProgramada: new Date("2019-11-05T16:00:00"),
	           duracionProgramadaMin: 60,
	           participaciones:[{numParticipacion:1, numEmpleado:1, tipoParticipacion:1, rol:"patrocinador"},
	        	   { numParticipacion:2, numEmpleado: 2, tipoParticipacion:2, rol:"scrum master"},
	        	   { numParticipacion:3, numEmpleado: 4, tipoParticipacion:2, rol:"scrum master"}
	           ]});
db.evento.insertOne({
	numEvento:4, nombre:"Junta de seguimiento mensual", 
	           objetivo:"Obtener lecciones aprendidas",
	           fechaProgramada: new Date("2019-11-25T9:00:00"),
	           duracionProgramadaMin: 120,
	           participaciones:[{numParticipacion:1, numEmpleado:1, tipoParticipacion:1, rol:"patrocinador", rol2:"moderador"},
	        	   { numParticipacion:2, numEmpleado: 2, tipoParticipacion:2, rol:"scrum master"},
	        	   { numParticipacion:3, numEmpleado: 3, tipoParticipacion:2, rol:"scrum master"}
	           ]});

// Encontrar los eventos del empleado 4
db.evento.find({"participaciones.numEmpleado":4});

//Encontrar las fechas, horas y duraciones de los eventos agendados para el empleado 4
db.evento.find({"participaciones.numEmpleado":4}).forEach(function(evt){
	print("fecha:"+evt.fechaProgramada + " " + evt.duracionProgramadaMin + " min");
});

//Encontrar las fechas, horas y duraciones de los eventos agendados para el empleado 4 y ponerlas en un objeto
db.evento.find({"participaciones.numEmpleado":4}).map(function(evt){
	return({fechaHora:evt.fechaProgramada, duracion:evt.duracionProgramadaMin});
});

//Encontrar las fechas, horas, duraciones y rol de los eventos agendados para el empleado 4 y ponerlas en un objeto
db.evento.find({"participaciones.numEmpleado":4}).map(function(evt){
	var rolEmp ="";
	var rol2Emp ="";
	for(let participI of evt.participaciones){
		if(participI.numEmpleado == 4){
			rolEmp=participI.rol;
			rol2Emp=participI.rol2;
		}
	}
	return({fechaHora:evt.fechaProgramada, duracion:evt.duracionProgramadaMin, rol:rolEmp, rol2:rol2Emp });
});

db.evento.find({"participaciones.numEmpleado":4}).map(function(evt){
	var rolEmp ="";
	var rol2Emp ="";
	for(let participI of evt.participaciones){
		if(participI.numEmpleado == 4){
			rolEmp=participI.rol;
			rol2Emp=participI.rol2;
		}
	}
	return({fechaHora:evt.fechaProgramada.toString(), duracion:evt.duracionProgramadaMin, rol:rolEmp, rol2:rol2Emp });
});
