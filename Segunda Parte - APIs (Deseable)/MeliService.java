package meli.business;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * @author Gloria.Ulloa
 *
 */
public class MeliService implements IMeliService {

	private Properties prop;

	public MeliService() throws Exception {
		// set props
		this.prop = obtenerArchivoPropiedades();

	}

	/**
	 * Metodo principal del service
	 */
	@Override
	public void consumirConsultas() {
		try {

			String urlBusqueda = prop.getProperty("urlBusqueda");
			String urlArticulo = prop.getProperty("urlArticulo");
			String maxTamanio = prop.getProperty("maxTamanio");
			String restriccionTamanio = prop.getProperty("restriccionTamanio");

			System.out.println("CONFIGURACIONES DEL PROGRAMA");
			System.out.println("urlBusqueda: " + urlBusqueda);
			System.out.println("urlArticulo: " + urlArticulo);
			System.out.println("maxTamanio: " + maxTamanio);
			System.out.println("restriccionTamanio: " + restriccionTamanio);

			if (urlBusqueda == null || urlArticulo == null || maxTamanio == null)
				System.out.println(
						"Los valores urlBusqueda, urlArticulo,restriccionTamanio y maxTamanio  son requeridos ");

			int numeroMaxTaminio = Integer.parseInt(maxTamanio);
			int numeroRestriccionTamanio = Integer.parseInt(restriccionTamanio);
			int numeroAConsultar = numeroMaxTaminio > numeroRestriccionTamanio ? numeroRestriccionTamanio : numeroMaxTaminio;

			int restante = (numeroMaxTaminio % 50);
			int iteraciones = (numeroMaxTaminio / 50);

			
			Integer numeroConsulta = 1;
			String valorConsulta = null;

			File archivoSalida = new File("resultadoConsulta.csv");
			archivoSalida.createNewFile();

			crearEncabezadosArchivoSalida(archivoSalida);

			do {

				String consulta = "consulta" + numeroConsulta;
				valorConsulta = prop.getProperty(consulta);

				if (valorConsulta == null)
					break;

				for (int i = 1; i <= iteraciones + 1; i++) {
					
					if (i==(iteraciones + 1) && restante==0)
						continue;
					
					if (i==(iteraciones + 1) && restante!=0)
						numeroAConsultar=restante;

					System.out.println(consulta + ": " + valorConsulta);
					String parametrosBusqueda = "?q=" + valorConsulta + "&limit=" + numeroAConsultar;
					
					System.out.println("parametrosBusqueda: " + parametrosBusqueda);

					JSONObject respuestaBusqueda = consumoRecurso(urlBusqueda + parametrosBusqueda);
					JSONArray resultados = new JSONArray(respuestaBusqueda.get("results").toString());

					FileWriter fw = null;
					PrintWriter pw = null;
					try {
						fw = new FileWriter(archivoSalida, true);
						pw = new PrintWriter(fw);

						for (Object articulo : resultados) {
							JSONObject articuloJSON = new JSONObject(articulo.toString());

							String parametrosArticulo = "/" + articuloJSON.get("id");
							JSONObject respuestaArticulo = consumoRecurso(urlArticulo + parametrosArticulo);

							gestionArchivoSalida(respuestaArticulo, archivoSalida, pw);

						}

						

					} finally {
						if (fw != null)
							fw.close();

						pw.close();

					}
				}
				
				numeroConsulta++;
				numeroAConsultar = numeroMaxTaminio > numeroRestriccionTamanio ? numeroRestriccionTamanio : numeroMaxTaminio;

			} while (valorConsulta != null);

			System.out.print("fin del programa");
		} catch (IOException ioe) {
			System.out.print("Ocurrio un error con el archivo de salida");
		}

		catch (Exception ex) {
			System.out.print("fin del programa con errores");

		}
	}

	/**
	 * Metodo para el consumo de los diferentes servicios API MELI
	 * 
	 * @param urlServicio
	 * @return
	 */
	private JSONObject consumoRecurso(String urlServicio) {

		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
		JSONObject jsonResp = new JSONObject();

		try {

			ResponseEntity<?> response = restTemplate.exchange(urlServicio, HttpMethod.GET, null, String.class);

			String resp = response.getBody().toString();
			jsonResp = new JSONObject(resp);

			if (response.getStatusCodeValue() == HttpStatus.OK.value()
					|| response.getStatusCodeValue() == HttpStatus.NO_CONTENT.value()) {

				System.out.println("Consumo exitoso " + urlServicio);
			}

		} catch (HttpClientErrorException e) {
			System.out.println("Error al consumir " + e);

		}

		return jsonResp;

	}

	/**
	 * Metodo para la escritura del encabezado del archivo de salida
	 * 
	 * @param file
	 * @throws IOException
	 * 
	 */
	private void crearEncabezadosArchivoSalida(File file) throws IOException {

		FileWriter fw = null;
		PrintWriter pw = null;

		try {
			fw = new FileWriter(file);
			pw = new PrintWriter(fw);

			Integer numeroColumna = 1;
			String valorColumna = null;
			String contenido = null;

			do {
				String columna = "columna" + numeroColumna;
				valorColumna = prop.getProperty(columna);

				if (valorColumna != null)
					contenido = valorColumna + ";";
				else
					contenido = "\n";

				pw.print(contenido);

				numeroColumna++;

			} while (valorColumna != null);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw e;
		} finally {
			if (fw != null)
				fw.close();

			pw.close();

		}

	}

	/**
	 * Metodo para gestionar la escritura en el archivo de salida
	 * 
	 * @param registro
	 * @param file
	 * @param pw
	 */
	private void gestionArchivoSalida(JSONObject registro, File file, PrintWriter pw) {

		Integer numeroColumna = 1;
		String valorColumna = null;
		String contenido = null;

		do {
			String columna = "columna" + numeroColumna;
			valorColumna = prop.getProperty(columna);

			if (valorColumna != null)
				contenido = (registro.get(valorColumna) != null ? registro.get(valorColumna).toString() : "null") + ";";
			else
				contenido = "\n";

			pw.print(contenido);

			numeroColumna++;

		} while (valorColumna != null);

	}

	/**
	 * metodo para cargar archivo de propiedades
	 * 
	 * @return
	 * @throws Exception
	 */
	private static Properties obtenerArchivoPropiedades() throws Exception {
		try {

			FileReader archivo = new FileReader("application.properties");

			Properties p = new Properties();

			p.load(archivo);
			return p;

		} catch (FileNotFoundException e1) {
			System.out.println(e1.getMessage());
			throw new Exception(
					"Archivo de propiedades no encontrado, asegurese de tener el archivo application.properties junto al jar");
		} catch (IOException e1) {

			throw new Exception("No fue posible leer el archivo de propiedades");
		}

	}

}