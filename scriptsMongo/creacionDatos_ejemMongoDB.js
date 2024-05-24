
/** Posicionarse en una base de datos -incluso inexistente- */ 
use('ejemMongoDB');
/** Insertar un documento en una colección lineal */ 
db.articulo.insertOne({cve_articulo:"A-23", descripcion:"Espejo lateral derecho VW Pointer 2003-2", 
	                   costo_prov_1:13.45, precio_lista:455.35});
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
db.venta.updateOne( { num_venta:1 }, 
		{ $push: {detalles : { numDetalle:4, cantidad:2, cve_articulo:"DR-57", 
								descripcion:"Amortiguador Ford Mustang 95-96",
								precio:945.34 } }
		});