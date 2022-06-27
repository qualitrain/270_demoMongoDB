package qtx.entidades;

import java.util.Date;
import java.util.HashMap;

public class Venta {
	private int numVenta;
	private Date fechaVenta;
	private Persona cliente;
	private Persona vendedor;
	private HashMap<Integer,DetalleVenta> detallesVta;

	
	public Venta(int numVenta, Date fechaVenta, Persona cliente,
			Persona vendedor) {
		super();
		this.numVenta = numVenta;
		this.fechaVenta = fechaVenta;
		this.cliente = cliente;
		this.vendedor = vendedor;
		this.detallesVta = new HashMap<Integer,DetalleVenta>();
	}
	public void agregarDetalle(int numDetalle, int cantidad, Articulo articulo, float precioUnitario){
		DetalleVenta nuevoDetalleVenta = new DetalleVenta (numDetalle, cantidad, articulo,precioUnitario);
		this.detallesVta.put(numDetalle, nuevoDetalleVenta); 
		}
	public HashMap<Integer, DetalleVenta> getDetallesVta() {
		return detallesVta;
	}
	public void setDetallesVta(HashMap<Integer, DetalleVenta> detallesVta) {
		this.detallesVta = detallesVta;
	}
	public int getNumVenta() {
		return numVenta;
	}
	public void setNumVenta(int numVenta) {
		this.numVenta = numVenta;
	}
	public Date getFechaVenta() {
		return fechaVenta;
	}
	public void setFechaVenta(Date fechaVenta) {
		this.fechaVenta = fechaVenta;
	}
	public Persona getCliente() {
		return cliente;
	}
	public void setCliente(Persona cliente) {
		this.cliente = cliente;
	}
	public Persona getVendedor() {
		return vendedor;
	}
	public void setVendedor(Persona vendedor) {
		this.vendedor = vendedor;
	}
	@Override
	public String toString() {
		return "Venta [numVenta=" + numVenta + ", fechaVenta=" + fechaVenta
				+ ", cliente=" + cliente + ", vendedor=" + vendedor
				+ ", detallesVta=" + detallesVta + "]";
	}
	public void mostrar() {
		float totalVenta=0;
		float totalDetalleVenta=0;
		if(this.numVenta <= 0){
			System.out.println("======================== Venta no actualizada en BD aún ===================");
			return;
		}
		System.out.println("===================================================================================================");
		System.out.println(" Venta num: "+this.numVenta+"                   Cliente:"+this.cliente.getNombre());
		System.out.println(" Fecha de venta:"+ this.fechaVenta.toString() 
		                 + "       Direccion:"+ this.cliente.getDireccion());
		System.out.println(" Vendedor: "+ this.vendedor.getIdPersona()+"-"+this.vendedor.getNombre());
		System.out.println("====================================================================================================");
		System.out.println(" num   cantidad   articulo                  descripcion                  precio unitario     total   ");
		System.out.println("====================================================================================================");
		for(int i=1;;i++){
			DetalleVenta det = this.detallesVta.get(i);
			if(det == null)
				break;
			totalDetalleVenta = det.getCantidad() * det.getPrecioUnitario();
			totalVenta+=totalDetalleVenta;
			System.out.print("  "+det.getNumDetalle()+"          ");
			System.out.print(+det.getCantidad()+"  ");
			System.out.print(String.format("%-15s",det.getArticuloVendido().getCveArticulo()) );
			System.out.print(" "+String.format("%-40s",det.getArticuloVendido().getDescripcion()));
			System.out.print("      "+String.format("%7.2f",det.getPrecioUnitario()));
			System.out.println("     "+String.format("%8.2f",totalDetalleVenta));
		}
		System.out.println("====================================================================================================");
		System.out.print("                                                                                total    ");
		System.out.println(String.format("%9.2f",totalVenta));
		System.out.println("====================================================================================================");

	}
	public double getTotal() {
		double total = 0;
		total = this.detallesVta
				    .values()
		            .stream()
		            .map(detI -> detI.getCantidad() * detI.getPrecioUnitario())
		            .reduce(0.0f, (acum, val) -> acum + val);
		return total;
	}


}
