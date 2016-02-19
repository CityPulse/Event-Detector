package com.siemens.ct.citypulse.event.detection.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.siemens.ct.citypulse.event.detection.resources.streamDescription.StreamDescription;

public class ResourceManagerConnector {
	
	private static Logger logger = Logger.getLogger(ResourceManagerConnector.class); 
	
	public static StreamDescription getStreamDescription(String inputStreamUUID) throws IOException {

		StreamDescription streamDescription;
		
		String URLString = Commons.GET_DESCRIPTION_URL;
		URLString = URLString.replace("#", Commons.resourceManagerConnectorIP+":"+Commons.resourceManagerConnectorPort);
		URLString = URLString + inputStreamUUID;
		
		URL urlToConnect = new URL(URLString);
		HttpURLConnection httpCon = (HttpURLConnection) urlToConnect.openConnection();
		// set http request headers
		httpCon.addRequestProperty("Host", "www.cumhuriyet.com.tr");
		httpCon.addRequestProperty("Connection", "keep-alive");
		httpCon.addRequestProperty("Cache-Control", "max-age=0");
		httpCon.addRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
		httpCon.addRequestProperty("User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36");
		httpCon.addRequestProperty("Accept-Encoding", "gzip,deflate,sdch");
		httpCon.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
		HttpURLConnection.setFollowRedirects(false);
		httpCon.setInstanceFollowRedirects(false);
		httpCon.setDoOutput(true);
		httpCon.setUseCaches(true);

		httpCon.setRequestMethod("GET");

		BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream(), "UTF-8"));
		String inputLine;
		StringBuilder message = new StringBuilder();
		while ((inputLine = in.readLine()) != null)
			message.append(inputLine);
		in.close();

		httpCon.disconnect();
		
		try{
			streamDescription = getStreamDescriptionObject(message.toString());
		}
		catch (JsonSyntaxException e)
		{
			logger.error("ResourceManagerConnection: Cannot convert JSON:"+ message.toString() +" to object: ", e);
			return null;
		}
				
		return streamDescription;
	}

	/**
	 * Method that maps a JSON to a StreamDescription object
	 * 
	 * @param responseFromUUIDString is the JSON stream description
	 * @return a StreamDescription object
	 */
	private static StreamDescription getStreamDescriptionObject(String responseFromUUIDString) {
		
		Gson gson = new Gson();
		StreamDescription streamDescription = gson.fromJson(responseFromUUIDString, StreamDescription.class);
		
		return streamDescription;
	}

}

