package qtx.entidades;

public class DetalleVenta {
	private int numDetalle;
	private int cantidad;
	private Articulo articuloVendido;
	private float precioUnitario;
	
	
	public DetalleVenta(int numDetalle, int cantidad, Articulo articuloVendido,
			float precioUnitario) {
		super();
		this.numDetalle = numDetalle;
		this.cantidad = cantidad;
		this.articuloVendido = articuloVendido;
		this.precioUnitario = precioUnitario;
	}
	public float getPrecioUnitario() {
		return precioUnitario;
	}
	public void setPrecioUnitario(float precioUnitario) {
		this.precioUnitario = precioUnitario;
	}
	public DetalleVenta(int numDetalle, int cantidad, Articulo articuloVendido) {
		super();
		this.numDetalle = numDetalle;
		this.cantidad = cantidad;
		this.articuloVendido = articuloVendido;
	}
	public int getNumDetalle() {
		return numDetalle;
	}
	public void setNumDetalle(int numDetalle) {
		this.numDetalle = numDetalle;
	}
	public int getCantidad() {
		return cantidad;
	}
	public void setCantidad(int cantidad) {
		this.cantidad = cantidad;
	}
	public Articulo getArticuloVendido() {
		return articuloVendido;
	}
	public void setArticuloVendido(Articulo articuloVendido) {
		this.articuloVendido = articuloVendido;
	}
	@Override
	public String toString() {
		return "DetalleVenta [numDetalle=" + numDetalle + ", cantidad="
				+ cantidad + ", articuloVendido=" + articuloVendido
				+ ", precioUnitario=" + precioUnitario + "]";
	}
	


}
