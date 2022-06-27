/**
 * 
 */
/** Consultar las bases de datos  */ 
show dbs;

/** Eliminar una base de datos  */ 
db.dropDatabase()

/** Posicionarse en una base de datos -incluso inexistente- */ 
use ejemMongoDB

/** Consultar en qué base de datos estamos posicionados para trabajar */ 
db

/** Insertar un documento en una colección lineal */ 
db.articulo.insertOne({cve_articulo:"A-23", descripcion:"Espejo lateral derecho VW Pointer 2003-2", 
	                   costo_prov_1:13.45, precio_lista:455.35});

/** Consultar los documentos en una coleccion */
db.articulo.find();

/** Consultar los documentos en una coleccion -formateada- */
db.articulo.find().pretty();

/** Insertar varios documentos en una colección lineal  */
db.articulo.insertMany([
	{cve_articulo:"A-24", descripcion:"Espejo lateral izquierdo VW Pointer 2003", 
		costo_prov_1:240.6, precio_lista:520.45},
	{cve_articulo:"D-F45-2231", descripcion:"Flecha Nissan Sentra 92-96", 
		costo_prov_1:563.5, precio_lista:1867.98},
	{cve_articulo:"DR-56", descripcion:"Amortiguador Ford Mustang 90-94", 
		costo_prov_1:482.5, precio_lista:945.34},
	{cve_articulo:"DR-57", descripcion:"Amortiguador Ford Mustang 95-96", 
		costo_prov_1:482.5, precio_lista:945.34},
	{cve_articulo:"X-1", descripcion:"Bujia Ford", 
		costo_prov_1:13.45, precio_lista:26.25}
	]);

/** Agregar un campo al primer documento de una coleccion */
db.articulo.update( {}, {$set:{status:"ok"}} );

/** Agregar un campo a todos los documento de una coleccion */
db.articulo.update( {}, {$set:{status:"ok"}}, {multi:true} );

/** Eliminar un campo en todos los documento de una coleccion */
db.articulo.update( {}, {$unset:{status:""}}, {multi:true} );

/** Consultar un documento, por un id de negocio, en una coleccion -formateada- */
db.articulo.find({cve_articulo:"D-F45-2231"}).pretty();

/** Consultar un documento, por un id físico -surrogado-, en una coleccion -formateada- */
db.articulo.find({_id: ObjectId("5d7c48aec7421a912bf1e112")}).pretty();

/** Consultar un documento por algún otro campo en una coleccion -formateada- */
db.articulo.find({precio_lista:945.34}).pretty();

/** Consultar un documento con criterios <,> en una coleccion -formateada- */
db.articulo.find( {precio_lista: {$lt: 945.34} } ).pretty();

/** Consultar un documento con criterios <,> , ordenada asc, en una coleccion -formateada- */
db.articulo.find( {precio_lista: {$lt: 1000.00} } ).sort({precio_lista:1}).pretty();

/** Actualizar un documento por Id en una coleccion */
db.articulo.update({cve_articulo:"A-23"},{$set:{costo_prov_1:210.9, precio_lista:457.0}});

/** Eliminar un documento por Id de una coleccion */
db.articulo.remove( {cve_articulo:"A-23"} );

/** Eliminar una coleccion */
db.articulo.drop();

/** Crear un índice ascendente sobre una colección */
db.articulo.createIndex( {cve_articulo:1} , {unique:true});

/** Crear un índice de texto sobre una colección */
db.articulo.createIndex( {descripcion:"text"} );


/** ================================================================ */
/** Insertar  colección persona  */

db.persona.insertMany([
	{id_persona:1, nombre:"Alejandro Ramírez De la Huerta", 
		direccion:"Av. Insurgentes Sur 456, col. Roma", fecha_nacimiento:new Date("1970-09-11")},
	{id_persona:2, nombre:"Jorge Fernández Menéndez", 
		direccion:"Salvador Díaz Miron 456, col. Del Valle", fecha_nacimiento:new Date("1954-12-31")},
	{id_persona:3, nombre:"Maricela de la Fuente Pérez", 
		direccion:"Margaritas 45, col. Villa de la Rueda", fecha_nacimiento:new Date("1980-02-23")},
	{id_persona:4, nombre:"Miguel Montes De la Paz", 
		direccion:"Benito Juárez 567 int 401, col. Héroes de Cha", fecha_nacimiento:new Date("1977-11-22")},
	{id_persona:5, nombre:"Brenda Berenice Torres Márquez", 
		direccion:"Av. Independencia 45, col. Centro", fecha_nacimiento:new Date("1983-04-07")}
	]);

/** Crear un índice ascendente sobre una colección */
db.persona.createIndex( {id_persona:1} , {unique:true});

/** Insertar  colección venta  */

db.venta.insertOne(
	{
		num_venta:1, fecha_venta:new Date(), cte_ObjId:ObjectId("5d7c50ebc7421a912bf1e118"), 
		vendedor_ObjId:ObjectId("5d7c50ebc7421a912bf1e11a"),
		detalles:[
			{
				numDetalle:1, cantidad:2, cve_articulo:"A-24", 
				descripcion:"Espejo lateral izquierdo VW Pointer 2003",
				precio:520.45
			},
			{
				numDetalle:2, cantidad:5, cve_articulo:"DR-56", 
				descripcion:"Amortiguador Ford Mustang 90-94",
				precio:945.34
			},
			{
				numDetalle:3, cantidad:8, cve_articulo:"X-1", 
				descripcion:"Bujia Ford",
				precio:26.25
			}
		]
	});

db.venta.insertOne(
	{
		num_venta:2, fecha_venta:new Date(), cte_ObjId:ObjectId("5d7c50ebc7421a912bf1e11b"), 
		vendedor_ObjId:ObjectId("5d7c50ebc7421a912bf1e11a"),
		detalles:[
			{
				numDetalle:1, cantidad:4, cve_articulo:"DR-57", 
				descripcion:"Amortiguador Ford Mustang 95-96",
				precio:945.34
			},
			{
				numDetalle:2, cantidad:4, cve_articulo:"X-1", 
				descripcion:"Bujia Ford",
				precio:26.25
			}
		]
		
	});
/** Crear un índice sobre la colección venta*/
db.venta.createIndex( {num_venta:1} , {unique:true});

/** Consulta sobre elementos incrustados  - un detalle en particular(completo!) - */
db.venta.find( { detalles:{numDetalle : 4, cantidad : 2, cve_articulo : "DR-57", descripcion : "Amortiguador Ford Mustang 95-96", precio : 945.34} }).pretty();

/** Consulta sobre elementos incrustados  - un campo en particular - */
db.venta.find( { 'detalles.numDetalle':{ $eq:3 } } ).pretty();

/** Consulta sobre elementos incrustados  - un campo en particular similar like SQL- */
db.venta.find( { 'detalles.descripcion':{ $regex:/^.*VW.*$/ } } ).pretty();

/** Agregar un campo "status" al arreglo detalles cuyo campo numDetalle = 2 y en la venta 2 */
db.venta.update( { num_venta:2, "detalles.numDetalle":2 }, { $set: {"detalles.$.status":"ok"}});

/** Agregar un campo "status" al todos los elementos del arreglo detalles de la venta 1 */
db.venta.update( { num_venta:1 }, { $set: {"detalles.$[].status":"ok"}});

/** Agregar un nuevo detalle a la venta 1 */
db.venta.update( { num_venta:1 }, 
		{ $push: {detalles : { numDetalle:4, cantidad:2, cve_articulo:"DR-57", 
								descripcion:"Amortiguador Ford Mustang 95-96",
								precio:945.34 } }
		});

/** Consultar solamente campos específicos */
db.articulo.find().forEach( function(art){ print ("cve artículo: " + art.cve_articulo); });

/** Consultar solamente campos específicos */
db.articulo.find().map( function(art){ 
							return { cve_articulo:art.cve_articulo, precio_lista:art.precio_lista } 
							}
                      );
/** Consultar solamente campos específicos y ligar con for-each */
db.articulo.find().map( function(art){ 
							return { cve_articulo:art.cve_articulo, precio_lista:art.precio_lista } 
							}
                      ).forEach(function(reg){ print ("{" + reg.cve_articulo + "," + reg.precio_lista + "}")});

/** Consultar solamente campos específicos, aplicando ordenamiento */
db.articulo.find().sort({cve_articulo:1}).forEach( function(art){ print ("cve artículo: " + art.cve_articulo); });

/** Buscar todas las ventas que tengan un artículo en particular */
db.venta.find ( {"detalles.cve_articulo": {$eq:"A-24"} } ).pretty();

/** Buscar todos los detalles de ventas que tengan un artículo en particular */
db.venta.find ( {"detalles.cve_articulo": {$eq:"DR-57"} } ).map( function(vta){ 
			var numDetalle = 0;
			var cantidad = 0;
			for(let detVtaI of vta.detalles){
				if(detVtaI.cve_articulo == "DR-57"){
					numDetalle = detVtaI.numDetalle;
					cantidad = detVtaI.cantidad;
					break;
				}
			}
        	return {num_venta:vta.num_venta, num_detalle:numDetalle, cantidad:cantidad}
        });

/** Map-reduce que deja un resumen de la venta, en realidad no reduce nada, solo mapea */
var map = function(){ 
	var total = 0;
	for(detalleI of this.detalles){
		total += detalleI.cantidad * detalleI.precio;
	}	
	emit(this.num_venta, total);
	};
var reduce = function(numVta, detalles){}
db.venta.mapReduce(map,reduce, { out: "resumenVta"});

/** Un join */
db.venta.aggregate([
	{ $lookup: {from:"persona", localField:"cte_ObjId", foreignField:"_id", as:"cliente"}}
]).pretty();

/** Un join más refinado */
db.venta.aggregate([
	{ $lookup: {from:"persona", localField:"cte_ObjId", foreignField:"_id", as:"cliente"}},
	{ $lookup: {from:"persona", localField:"vendedor_ObjId", foreignField:"_id", as:"vendedor"}},
	{ $project: {_id:false, cte_ObjId:false, detalles:false, vendedor_ObjId:false}}
]).map(function(vta){
	     return {numvta: vta.num_venta, fecha: vta.fecha_venta, vendedor:vta.vendedor[0].nombre, 
	    	     cliente: vta.cliente[0].nombre } 
       });

/** Consultar el num_venta mayor o menor */
db.venta.find().sort({num_venta:-1}).limit(1).pretty(); // para MAX
db.venta.find().sort({num_venta:1}).limit(1).pretty();  // para MIN

/** Eliminar una venta x ObjectId  */
db.venta.remove( {_id: ObjectId("5d818f3d871094584f524724") } );


/** Crear un esquema en JavaScript */
db.createCollection("cliente", {
	validator: {
	  $jsonSchema: {
		bsonType: "object",
		required: ["numCte", "tipo", "nombre", "direccion", "saldo"]
		}
	}
}
);

/** probando la validación del esquema con dos inserciones, una correcta y otra equivocada */
db.cliente.insertOne({numCte:1, tipo:"mayoreo", nombre: "Elías Pedrero Goya", direccion:"Av. de la Paz 551",
	saldo:0.0});

db.cliente.insertOne({nmCte:2, nombre: "Jorge de la Hoya Mota", direccion:"Río Verde 900 - 201",
	saldo:55.0});

/** Crear un esquema en JavaScript con especificación de campos*/
db.createCollection("cliente", 
{
	validator: {
	  $jsonSchema: {
		bsonType: "object",
		required: ["numCte", "tipo", "nombre", "direccion", "saldo"],
		properties:  { numCte: { type:"number", minimum:1, description:"debe ser un entero positivo"} }
	               }
              }
});

/** probando la validación del esquema con dos inserciones, una correcta y otra equivocada */
db.cliente.insertOne({numCte:1, tipo:"mayoreo", nombre: "Elías Pedrero Goya", direccion:"Av. de la Paz 551",
	saldo:0.0});

db.cliente.insertOne({numCte:0, tipo:"mayoreo", nombre: "Jorge Salinas", direccion:"Av. de la Paz 531",
	saldo:0.0});

/** Crear un esquema en JavaScript con especificación de campos*/
db.createCollection("cliente", 
{
	validator: {
	  $jsonSchema: {
		bsonType: "object",
		required: ["numCte", "tipo", "nombre", "direccion", "saldo"],
		properties:  { numCte: { type:"number", minimum:1, description:"debe ser un entero positivo"},
			           tipo: { 	enum: ["mayoreo","menudeo"], description: "solo puede tomar valores de mayoreo, menudeo"} }
	               }
              }
});

/** probando la validación del esquema con dos inserciones, una correcta y otra equivocada */
db.cliente.insertOne({numCte:1, tipo:"mayoreo", nombre: "Elías Pedrero Goya", direccion:"Av. de la Paz 551",
	saldo:0.0});

db.cliente.insertOne({numCte:2, tipo:"mayor", nombre: "Jorge Salinas", direccion:"Av. de la Paz 531",
	saldo:0.0});


