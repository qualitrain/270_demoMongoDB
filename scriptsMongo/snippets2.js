/**
 * 
 */

// Inserción de documentos en colección empleado
db.empleado.insertOne({numEmpleado:1,nombre:"Alex",departamento:"ventas"});

db.empleado.insertOne({num:2,nombrecito:"Jorgito",departamento:"ventas"});
// Consultar todos los documentos de empleado

db.empleado.find();
db.empleado.find().pretty();

db.empleado.insertOne({numEmpleado:1,nombre:"Alex",departamento:"ventas", sueldo:15000});

db.empleado.insertOne({_id:1,nombre:"Alex",departamento:"ventas", sueldo:15000});

db.empleado.insertOne({num:3,nombre:"Mariana",departamento:{nombre:"compras", responsable:"Juan"},telefono:"55-34-56-77-12"});
