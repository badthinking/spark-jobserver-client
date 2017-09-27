package util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;

import lombok.extern.log4j.Log4j;
import okhttp3.*;

@Log4j
public class Http {
	private static OkHttpClient client = new OkHttpClient.Builder().connectTimeout(60, TimeUnit.SECONDS)
			.writeTimeout(600, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS).build();

	public static String get(String url) throws IOException {
		log.info("GET " + url);
		Request request = new Request.Builder().url(url).get().build();
		return processRequest(request);
	}
	
	public static String postJson(String url, String json) throws IOException {
		return post("json", url, json);
	}

	public static String postJar(String url, byte[] data) throws IOException {
		return post("jar", url, data);
	}

	public static String postFile(String url, File file) throws IOException {
		return post("binary", url, IOUtils.toByteArray(new FileInputStream(file)));
	}

	public static String delete(String url) throws IOException {
		log.info("DELETE " + url);
		Request request = new Request.Builder().url(url).delete().build();
		return processRequest(request);
	}

	private static String post(String type, String url, Object data) throws IOException {
		log.info("POST " + url);

		RequestBody body = null;
		switch (type) {
		case "json":
			body = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), (String) data);
			break;
		case "jar":
			body = RequestBody.create(MediaType.parse("application/java-archive"), (byte[]) data);
			break;
		case "binary":
			body = RequestBody.create(MediaType.parse("application/octet-stream"), (byte[]) data);
			break;
		default:
			// other
		}

		Request request = new Request.Builder().url(url).post(body).build();
		return processRequest(request);
	}

	private static String processRequest(Request request) throws IOException {
		try (Response response = client.newCall(request).execute()) {
			String result = response.body().string();
			log.debug(result);
			return result;
		}
	}
}