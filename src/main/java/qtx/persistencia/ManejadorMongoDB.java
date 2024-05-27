package qtx.persistencia;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.TransactionBody;

import qtx.entidades.Articulo;
import qtx.entidades.DetalleVenta;
import qtx.entidades.Persona;
import qtx.entidades.Venta;
/**
 * Para 
 * Mongo 6+
 * mongodb-driver-sync 5.1+
 * Java 17+
 */
public class ManejadorMongoDB {
	private static final String BASE_DATOS = "ejemMongoDB";
	private static final String SERVIDOR01 = "localhost";
	private static final int puerto01 = 27017;
	
	private static MongoClient poolConexionesMongo = null;
	
	public static  MongoClient getMongoCliente_Driver_sync_5() {
		
		List<ServerAddress> lstServidoresMongo = List.of(new ServerAddress(SERVIDOR01, puerto01));
		
	    MongoClientSettings settings = 
	    	MongoClientSettings.builder()
	                             .applyToClusterSettings(builder -> builder.hosts(lstServidoresMongo))
	                             .serverApi(ServerApi.builder()
	                                                   .version(ServerApiVersion.V1)
	                                                   .build())
	                             .build();

	    MongoClient mongoClient = MongoClients.create(settings);
	    return mongoClient;
	}	

	public static void abrirPoolConexiones() {
		ManejadorMongoDB.poolConexionesMongo = getMongoCliente_Driver_sync_5();
	}
	
	public static  MongoDatabase conectarBD() {
		return poolConexionesMongo.getDatabase(BASE_DATOS);
	}
	
	public static void cerrarConexiones() {
		if(ManejadorMongoDB.poolConexionesMongo != null) {
			ManejadorMongoDB.poolConexionesMongo.close();
		}
	}
	
	public static List<String> getClavesArticulo() {
		List<String> clavesArticulo = new ArrayList<String>();
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
//		Consumer<Document> agregadorCveArt = (Document art) -> clavesArticulo.add((String) art.get("cve_articulo"));
		collArticulos.find()
		             .forEach((Document art) -> clavesArticulo.add((String) art.get("cve_articulo")));
//		             .forEach(agregadorCveArt);

		return clavesArticulo;
	}
	
	public static List<Articulo> getArticulos() {
		List<Articulo> articulos = new ArrayList<Articulo>();
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		collArticulos.find()
					 .sort(Sorts.ascending("descripcion"))
		             .forEach((Document art) -> agregarArticulo(art,articulos));
		return articulos;
	}
	
	public static List<Articulo> getArticulosMasCarosQue(float importe) {
		List<Articulo> articulos = new ArrayList<Articulo>();
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		collArticulos.find(Filters.gt("precio_lista", importe))
					 .sort(Sorts.ascending("descripcion"))
		             .forEach((Document art) -> agregarArticulo(art,articulos));
		return articulos;
	}
	
	private static void agregarArticulo(Document docArticulo, List<Articulo> articulos) {
		Articulo articulo;
		articulo = new Articulo();
		articulo.setCveArticulo( docArticulo.getString("cve_articulo") );
		articulo.setDescripcion( docArticulo.getString("descripcion"));
		articulo.setCostoProv1((float)( docArticulo.getDouble("costo_prov_1") * 1) );
		articulo.setPrecioLista((float)( docArticulo.getDouble("precio_lista") * 1) );
		articulos.add(articulo);
	}
	
	public static Articulo getArticuloXID(String cveArticulo) {
		Articulo articulo = null;
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		Document docArticulo = collArticulos.find(
//				Filters.eq("cve_articulo", cveArticulo)
				new Document("cve_articulo", cveArticulo)
				).first();
		if(docArticulo != null) {
			articulo = new Articulo();
			articulo.setCveArticulo( docArticulo.getString("cve_articulo") );
			articulo.setDescripcion( docArticulo.getString("descripcion"));
			articulo.setCostoProv1((float)( docArticulo.getDouble("costo_prov_1") * 1) );
			articulo.setPrecioLista((float)( docArticulo.getDouble("precio_lista") * 1) );
		}
		return articulo;
	}
	
	public static int insertarArticulo(Articulo articulo) {
		Document docArticulo = new Document();
		docArticulo.append("cve_articulo", articulo.getCveArticulo())
		           .append("descripcion", articulo.getDescripcion())
		           .append("costo_prov_1", articulo.getCostoProv1())
		           .append("precio_lista", articulo.getPrecioLista());
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		try {
			collArticulos.insertOne(docArticulo);
		}catch(MongoException mex) {
			System.out.println(mex.getMessage() + ", code:" + mex.getCode());
			return 0;
		}
		return 1;
	}
	
	public static int modificarArticulo(Articulo articulo) {
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		try {
			UpdateResult resultadoUpdate = 
				collArticulos.updateOne(
					            Filters.eq("cve_articulo", articulo.getCveArticulo()), 
					            Updates.combine(Updates.set("descripcion", articulo.getDescripcion()),
							                    Updates.set("costo_prov_1", articulo.getCostoProv1()),
							                    Updates.set("precio_lista", articulo.getPrecioLista())
							)
					);
			return (int) resultadoUpdate.getModifiedCount();
		}catch(MongoException mex) {
			System.out.println(mex.getMessage() + ", code:" + mex.getCode());
			return 0;
		}
	}
	
	public static int remplazarArticulo(Articulo articulo) {
		Document docArticulo = new Document();
		docArticulo.append("cve_articulo", articulo.getCveArticulo())
		           .append("descripcion", articulo.getDescripcion())
		           .append("costo_prov_1", articulo.getCostoProv1())
		           .append("precio_lista", articulo.getPrecioLista());

		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		try {
			UpdateResult resultadoUpdate = 
				collArticulos.replaceOne(
					            Filters.eq("cve_articulo", articulo.getCveArticulo()), docArticulo);
			return (int) resultadoUpdate.getModifiedCount();
		}catch(MongoException mex) {
			System.out.println(mex.getMessage() + ", code:" + mex.getCode());
			return 0;
		}
	}
	
	public static int eliminarArticulo(String cveArticulo) {
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collArticulos = conexionBD.getCollection("articulo");
		try {
			DeleteResult resultadoDelete = 
				collArticulos.deleteOne(
					            Filters.eq("cve_articulo", cveArticulo));
			return (int) resultadoDelete.getDeletedCount();
		}catch(MongoException mex) {
			System.out.println(mex.getMessage() + ", code:" + mex.getCode());
			return 0;
		}
	}
	
	public static Persona getPersonaXID(int idPersona) {
		Persona persona = null;
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collPersonas = conexionBD.getCollection("persona");
		Document docPersona = collPersonas.find(
				Filters.eq("id_persona", idPersona)
				).first();
		if(docPersona != null) {
			String nombre = docPersona.getString("nombre");			
			String direccion = docPersona.getString("direccion");
			Date fechaNacimiento = docPersona.getDate("fecha_nacimiento");
			persona = new Persona(idPersona,nombre,direccion,fechaNacimiento);
		}
		return persona;
	}
	
	public static Persona getPersonaXObjectID(ObjectId _id) {
		Persona persona = null;
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collPersonas = conexionBD.getCollection("persona");
		Document docPersona = collPersonas.find(
				Filters.eq("_id", _id)
				).first();
		if(docPersona != null) {
			int idPersona = ((Number) docPersona.get("id_persona")).intValue();	 
			String nombre = docPersona.getString("nombre");			
			String direccion = docPersona.getString("direccion");
			Date fechaNacimiento = docPersona.getDate("fecha_nacimiento");
			persona = new Persona(idPersona,nombre,direccion,fechaNacimiento);
		}
		return persona;
	}
	
	public static Venta getVentaXID(int numVenta) {
		Venta venta = null;
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collVentas = conexionBD.getCollection("venta");
		Document docVenta = collVentas.find(Filters.eq("num_venta", numVenta))
				                      .first();
		if(docVenta != null) {
			Date fechaVenta = docVenta.getDate("fecha_venta");			
			ObjectId cteObjId = docVenta.getObjectId("cte_ObjId");
			Persona cliente = ManejadorMongoDB.getPersonaXObjectID(cteObjId);
			
			ObjectId vendedorObjId = docVenta.getObjectId("vendedor_ObjId");
			Persona vendedor = ManejadorMongoDB.getPersonaXObjectID(vendedorObjId);
			
			venta = new Venta(numVenta, fechaVenta, cliente, vendedor);
			List<Document> detalles = docVenta.getList("detalles", Document.class);
			
			for(Document docDetalleI : detalles) {
				int numDetalle = ((Number) docDetalleI.get("numDetalle")).intValue();	
				int cantidad = ((Number) docDetalleI.get("cantidad")).intValue();
				String cveArticulo = docDetalleI.getString("cve_articulo");
				float precio = (float)(docDetalleI.getDouble("precio") * 1);
				Articulo articulo = ManejadorMongoDB.getArticuloXID(cveArticulo);
				venta.agregarDetalle(numDetalle, cantidad, articulo, precio);
			}
		}
		return venta;
	}
	
	public static int insertarVentaTransaccional(Venta nuevaVenta) {
		// REQUIERE UN REPLICA SET PARA FUNCIONAR

		AtomicInteger numVta = new AtomicInteger();
		/* Paso 1: Iniciar una sesi�n cliente. */

		ClientSession clientSession = poolConexionesMongo.startSession();

		/* Paso 2: Opcional. Configurar opciones de la transacci�n. */

		TransactionOptions txnOptions = 
				          TransactionOptions.builder()
									        .readPreference(ReadPreference.primary())
									        .readConcern(ReadConcern.MAJORITY)
									        .writeConcern(WriteConcern.MAJORITY)
									        .build();
		
		/* Paso 3: Definir operaciones que forman la transacci�n. */

		TransactionBody<String> transaccion = 
				() -> { 
						MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
						MongoCollection<Document> collVentas = conexionBD.getCollection("venta");
						MongoCollection<Document> collClientes = conexionBD.getCollection("persona");
						
						double totalVta = nuevaVenta.getTotal();
						int idPersonaCte = nuevaVenta.getCliente()
								                     .getIdPersona();
						int idPersonaVendedor = nuevaVenta.getVendedor()
			                                              .getIdPersona();
						/* ----- Obtener los ObjectId(s) a insertar --------------*/
						
						ObjectId cteObjId = 
						    collClientes.find(clientSession, Filters.eq("id_persona", idPersonaCte) )
						                            .first()
						                            .getObjectId("_id");
						ObjectId vendedorObjId = 
							collClientes.find(clientSession, Filters.eq("id_persona", idPersonaVendedor) )
                                                    .first()
                                                    .getObjectId("_id");
						
						/* ----- Calcular el num_venta a insertar --------------*/
						int numVtaNext = ((Number)collVentas.find(clientSession)
						          .sort(Sorts.descending("num_venta"))
						          .limit(1)
						          .first()
						          .get("num_venta")).intValue() + 1;
						
						/* ----- Insertar la venta --------------*/
						
						Document docVenta = generarDocumentVenta(nuevaVenta, cteObjId, vendedorObjId, numVtaNext);
						collVentas.insertOne(clientSession,docVenta);
						/* ----- Actualizar saldo cliente ------------------*/
						collClientes.updateOne(clientSession, Filters.eq("id_persona", idPersonaCte), 
								               Updates.inc("saldo", totalVta) );
						numVta.addAndGet(numVtaNext);
						return "ok";
					  };
		
		/* Paso 4: invocar ejecutor de transacciones */
		/*         ejecutar el callback y el commit / rollback */
					  
		try {
			String status = clientSession.withTransaction(transaccion, txnOptions);
			System.out.println("status:" + status);
			return numVta.get();
		}
		catch(Exception ex) {
			System.out.println("Transaccion abortada:"+ex.getMessage());
			return 0;		
		}
	}
	
	private static Document generarDocumentVenta(Venta vta, ObjectId cteObjId, ObjectId vendedorObjId, int numVtaNext) {
		Document docVenta = new Document();
		docVenta.append("num_venta", numVtaNext)
		        .append("fecha_venta", new Date())
		        .append("cte_ObjId", cteObjId)
				.append("vendedor_ObjId", vendedorObjId);
		
		List<DetalleVenta> detalles = new ArrayList<>();
		vta.getDetallesVta()
		   .values()
		   .stream()
		   .sorted( (det1,det2) -> ( det1.getNumDetalle() < det2.getNumDetalle() ? -1 : 1) )
		   .forEach(det-> detalles.add(det));
		
		List<BsonDocument> docDetalles = new ArrayList<>();
		for(DetalleVenta detalleI : detalles) {
			BsonDocument docDetalle = new BsonDocument();
			docDetalle.append("numDetalle", new BsonInt32(detalleI.getNumDetalle()) )
			          .append("cantidad", new BsonInt32(detalleI.getCantidad()))
			          .append("cve_articulo", new BsonString(detalleI.getArticuloVendido().getCveArticulo()))
			          .append("descripcion", new BsonString(detalleI.getArticuloVendido().getDescripcion()))
			          .append("precio", new BsonDouble(detalleI.getPrecioUnitario()));
			docDetalles.add(docDetalle);
		}		
		BsonArray arrBsonDetalles = new BsonArray(docDetalles);
		
		docVenta.append("detalles", arrBsonDetalles);
		return docVenta;
	}
	
	public static int actualizarSaldoCliente(Persona cliente, double importe) {
		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collClientes = conexionBD.getCollection("persona");
		try {
			UpdateResult resultadoUpdate = 
					collClientes.updateOne(
					            Filters.eq("id_persona", cliente.getIdPersona()), 
					            Updates.inc("saldo", importe)
							);
			return (int) resultadoUpdate.getModifiedCount();
		}catch(MongoException mex) {
			System.out.println(mex.getMessage() + ", code:" + mex.getCode());
			return 0;
		}
	}
	
	public static int insertarVenta(Venta nuevaVenta) {

		MongoDatabase conexionBD = ManejadorMongoDB.conectarBD();
		MongoCollection<Document> collVentas = conexionBD.getCollection("venta");
		MongoCollection<Document> collClientes = conexionBD.getCollection("persona");
		
		double totalVta = nuevaVenta.getTotal();
		int idPersonaCte = nuevaVenta.getCliente()
				                     .getIdPersona();
		int idPersonaVendedor = nuevaVenta.getVendedor()
                                          .getIdPersona();
		/* ----- Obtener los ObjectId(s) a insertar --------------*/
		
		ObjectId cteObjId = 
		    collClientes.find(Filters.eq("id_persona", idPersonaCte) )
		                            .first()
		                            .getObjectId("_id");
		ObjectId vendedorObjId = 
			collClientes.find(Filters.eq("id_persona", idPersonaVendedor) )
                                    .first()
                                    .getObjectId("_id");
		
		/* ----- Calcular el num_venta a insertar --------------*/
		int numVtaNext = ((Number)collVentas.find()
		          .sort(Sorts.descending("num_venta"))
		          .limit(1)
		          .first()
		          .get("num_venta")).intValue() + 1;
		
		/* ----- Insertar la venta --------------*/
		
		Document docVenta = generarDocumentVenta(nuevaVenta, cteObjId, vendedorObjId, numVtaNext);
		collVentas.insertOne(docVenta);
		/* ----- Actualizar saldo cliente ------------------*/
		collClientes.updateOne(Filters.eq("id_persona", idPersonaCte), 
				               Updates.inc("saldo", totalVta) );
		return numVtaNext;
	}

}
