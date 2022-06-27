package qtx.test;

import java.util.List;

import qtx.entidades.Articulo;
import qtx.persistencia.ManejadorMongoDB;

public class Cliente01MongoDB {
	public static void testConsultaClavesArt_todas() {
		System.out.println("\n=============== "+ "testConsultaClavesArt_todas()" + " ===============");
		System.out.println("claves de artículos: " + ManejadorMongoDB.getClavesArticulo());
	}
	public static void testConsultaArt_todos() {
		System.out.println("\n=============== "+ "testConsultaArt_todas()" + " ===============");
		List<Articulo> articulos = ManejadorMongoDB.getArticulos();
		for(Articulo artI : articulos)
			System.out.println(artI);
	}
	public static void testConsultaArt_porPrecio() {
		System.out.println("\n=============== "+ "testConsultaArt_porPrecio()" + " ===============");
		List<Articulo> articulos = ManejadorMongoDB.getArticulosMasCarosQue(500.0f);
		for(Articulo artI : articulos)
			System.out.println(artI);
	}
	private static void testConsultaXID() {
		System.out.println("\n==================== "+ "testConsultaXID()" + " ====================");
		Articulo articulo = ManejadorMongoDB.getArticuloXID("A-24");
		System.out.println("Artículo recuperado por ID: " + articulo);
	}
	public static void testInsercion() {
		System.out.println("\n==================== "+ "testInsercion()" + " ====================");
		Articulo articulo = new Articulo("ACR-01","Tapón de Gasolina Nissan Tsuru 1998",325.5f, 699.90f);
		int nIserciones = ManejadorMongoDB.insertarArticulo(articulo);
		System.out.println("Se han insertado " + nIserciones + " registros");
	}
	private static void testModificacion() {
		System.out.println("\n==================== "+ "testModificacion()" + " ====================");
		Articulo articulo = ManejadorMongoDB.getArticuloXID("ACR-01");
		float costoNuevo = articulo.getCostoProv1() * 1.40f;
		float precioNuevo = articulo.getPrecioLista() * 1.40f;
		articulo.setCostoProv1(costoNuevo);
		articulo.setPrecioLista(precioNuevo);
		int nModif = ManejadorMongoDB.modificarArticulo(articulo);
		System.out.println("Se han modificado " + nModif + " registros");
	}
	private static void testRemplazo() {
		System.out.println("\n==================== "+ "testRemplazo()" + " ====================");
		Articulo articulo = ManejadorMongoDB.getArticuloXID("ACR-01");
		float costoNuevo = articulo.getCostoProv1() * 1.40f;
		float precioNuevo = articulo.getPrecioLista() * 1.40f;
		articulo.setCostoProv1(costoNuevo);
		articulo.setPrecioLista(precioNuevo);
		int nModif = ManejadorMongoDB.remplazarArticulo(articulo);
		System.out.println("Se han modificado " + nModif + " registros");
	}
	private static void testEliminacion() {
		System.out.println("\n==================== "+ "testEliminacion()" + " ====================");
		int nBorrados = ManejadorMongoDB.eliminarArticulo("ACR-01");
		System.out.println("Se han eliminado " + nBorrados + " registros");
	}

	public static void main(String[] args) {
		testConsultaClavesArt_todas();
		testConsultaArt_todos();
		testConsultaXID();
		testInsercion();
		testConsultaArt_todos();
		testModificacion();
		testConsultaArt_todos();
		testRemplazo();
		testConsultaArt_todos();
		testEliminacion();
		testConsultaArt_todos();
		testConsultaArt_porPrecio();
	}

}
