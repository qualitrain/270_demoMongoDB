package qtx.test;

import qtx.entidades.Articulo;
import qtx.entidades.Persona;
import qtx.entidades.Venta;
import qtx.persistencia.ManejadorMongoDB;

public class TestTransacciones {

	public static void testTransaccionNuevaVenta() {
		Persona unVendedor = ManejadorMongoDB.getPersonaXID(1);
		Persona unCliente = ManejadorMongoDB.getPersonaXID(2);
		System.out.println("vendedor:" + unVendedor);
		System.out.println("cliente:" + unCliente);
		
		Venta nuevaVenta = new Venta(0,null,unCliente,unVendedor);
		Articulo articulo1 =  ManejadorMongoDB.getArticuloXID("A-23");
		nuevaVenta.agregarDetalle(1, 5, articulo1,articulo1.getPrecioLista());
		
		Articulo articulo2 = ManejadorMongoDB.getArticuloXID("A-24");
		nuevaVenta.agregarDetalle(2, 5, articulo2,articulo2.getPrecioLista());
		
		Articulo articulo3 = ManejadorMongoDB.getArticuloXID("X-1");
		nuevaVenta.agregarDetalle(3, 8, articulo3,articulo3.getPrecioLista());
		
		Articulo articulo4 = ManejadorMongoDB.getArticuloXID("D-F45-2231");
		nuevaVenta.agregarDetalle(4, 6, articulo4,articulo4.getPrecioLista());

		Articulo articulo5 = ManejadorMongoDB.getArticuloXID("DR-56");
		nuevaVenta.agregarDetalle(5, 4, articulo5,articulo5.getPrecioLista());
		
//		int numVenta = ManejadorMongoDB.insertarVentaTransaccional(nuevaVenta);
		int numVenta = ManejadorMongoDB.insertarVentaNoTransaccional(nuevaVenta);
		System.out.println("Venta insertada con numVenta = " + numVenta);
		Venta ventaInsertada = ManejadorMongoDB.getVentaXID(numVenta);
		ventaInsertada.mostrar();
	}
	public static void testGetVentaXID() {
		Venta venta = ManejadorMongoDB.getVentaXID(1);
		venta.mostrar();
	}
	public static void main(String[] args) {
//		testTransaccionNuevaVenta();
		testGetVentaXID();
	}

}
