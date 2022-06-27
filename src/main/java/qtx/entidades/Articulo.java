package qtx.entidades;

public class Articulo {
	private String cveArticulo;
	private String descripcion;
	private float costoProv1;
	private float precioLista;

	public Articulo() {
		super();
	}

	public Articulo(String cveArticulo, String descripcion, float costoProv1,
			float precioLista) {
		super();
		this.cveArticulo = cveArticulo;
		this.descripcion = descripcion;
		this.costoProv1 = costoProv1;
		this.precioLista = precioLista;
	}

	public Articulo(String cveArticulo) {
		super();
		this.cveArticulo = cveArticulo;
	}

	public String getCveArticulo() {
		return cveArticulo;
	}

	public void setCveArticulo(String cveArticulo) {
		this.cveArticulo = cveArticulo;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public float getCostoProv1() {
		return costoProv1;
	}

	public void setCostoProv1(float costoProv1) {
		this.costoProv1 = costoProv1;
	}

	public float getPrecioLista() {
		return precioLista;
	}

	public void setPrecioLista(float precioLista) {
		this.precioLista = precioLista;
	}

	@Override
	public String toString() {
		return "Articulo [cveArticulo=" + cveArticulo + ", descripcion="
				+ descripcion + ", costoProv1=" + costoProv1 + ", precioLista="
				+ precioLista + "]";
	}
}
