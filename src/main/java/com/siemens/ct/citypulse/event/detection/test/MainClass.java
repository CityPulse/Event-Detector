package com.siemens.ct.citypulse.event.detection.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.siemens.ct.citypulse.event.detection.api.EventDetection;
import com.siemens.ct.citypulse.event.detection.core.Commons;

import citypulse.commons.data.Coordinate;

public class MainClass {
	
	static Logger logger = Logger.getLogger(MainClass.class);
	static int RMchecks = 1;

	public static void main(String[] args) {
		
		logger.info("Checking Resource Management status");
		System.out.println("Checking Resource Management status");
		while(testRequestForResourceManagement()==false)
		{
			
			if(RMchecks>10)
			{
				logger.error("Resource Management failed to loaded after "+RMchecks+" attempts. ED cannot start");
				System.out.println("Resource Management failed to loaded after "+RMchecks+" attempts. ED cannot start");
				System.exit(1);
			}
			RMchecks++;
			
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				logger.error("Problems occured when sleeping the EventDetection thread", e);
			}
			
		}
		logger.info("Resource Management loaded after "+RMchecks+" attempts, proceeding to Event detection launch");
		System.out.println("Resource Management loaded after "+RMchecks+" attempts, proceeding to Event detection launch");
		
		EventDetection eventDetection = new EventDetection();
				
		//reading the UUIDs from file
		String lineFormFile;
		int i = 0;

		//reading the traffic sensors UUIDs and coordinates from file.
		BufferedReader reader = new BufferedReader(new InputStreamReader(MainClass.class.getResourceAsStream(Commons.AARHUS_TRFFIC_UUIDs)));
		
		try {
			while ((lineFormFile = reader.readLine()) != null) {
					
				//parsing the UUID, lat and long 
				String uuid = lineFormFile.substring(0, lineFormFile.indexOf(","));
				
				//this sensor has some problems in SPARQL endpoint
				if(!uuid.equals("847938fe-5d03-5c36-913d-31e4d2b082b5"))
				{
					
					if(!uuid.equals("80043a6c-3bdb-5e47-9199-151fb8cfd619"))
					{
						//continue;
					}
					//getting coordinates
					String coordinates = lineFormFile.substring(lineFormFile.indexOf(",")+1, lineFormFile.length());
					String lon = coordinates.substring(0, coordinates.indexOf(" "));
					String lat = coordinates.substring(coordinates.indexOf(" ")+1, coordinates.length() );
					
					// eventDetectionLogicInputMapping hashmap name (trafficDataSource) + uuid
					HashMap<String, String> inputMappingTraffic = new HashMap<String, String>();
					inputMappingTraffic.put("trafficDataSource", uuid);
					
					HashMap<String, String> inputMappingAarhusPollution = new HashMap<String, String>();
					inputMappingAarhusPollution.put("aarhusPollution", uuid);
					
					HashMap<String, String> inputMappingAarhusNoiseLevel = new HashMap<String, String>();
					inputMappingAarhusNoiseLevel.put("aarhusNoiseLevel", uuid);

					//this will be the eventName parameter in the event detection nodes.
					//eventName contains the string "SENSOR_UUID1_UUID2_.." basically all the UUIDs that form 
					//that particular node
					String eventName = getEventName(inputMappingTraffic);
					
					// trafficJamEventDetectionNode object
					TrafficJamEventDetectionNode trafficJamNode_1 = new TrafficJamEventDetectionNode(i + "", "TrafficJam",
							eventName, uuid, "35", "10", "24");
					
					eventName = getEventName(inputMappingAarhusPollution);
					
					AarhusPollutionEventDetectionNode aarhusPollutionNode = new AarhusPollutionEventDetectionNode(
							i + "_aarhusPollution", "AarhusPollution", eventName, uuid, "10", "25");
					
					eventName = getEventName(inputMappingAarhusNoiseLevel);

					// the threshold values are estimates by using the
					// aarhusPollutionNode thresholds of 10 and 25 cars, and the
					// speed of 25 km/s. So, 10*25 = 250 and 25*25 = 625.
					AarhusNoiseLevelEventDetectionNode aarhusNoiseLevelNode = new AarhusNoiseLevelEventDetectionNode(
							i + "_aarhusNoiseLevelNode", "AarhusNoiseLevel", eventName, uuid, "250", "625");
					
					//setting the coordinates
					Coordinate coordinate = new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat));
					trafficJamNode_1.setEventCoordinate(coordinate);
					aarhusPollutionNode.setEventCoordinate(coordinate);
					aarhusNoiseLevelNode.setEventCoordinate(coordinate);
					
					//adding the node to ED
					eventDetection.addEventDetectionNode(inputMappingTraffic, trafficJamNode_1);
					eventDetection.addEventDetectionNode(inputMappingAarhusPollution, aarhusPollutionNode);
					eventDetection.addEventDetectionNode(inputMappingAarhusNoiseLevel, aarhusNoiseLevelNode);
					
					i++;
					System.out.println("Added "+i+" traffic sensors." + uuid);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("Could not open csv file with traffic sensor data");
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Problems with the the data from the csv line "+i+". Please check if UUID is still valid");
			e.printStackTrace();
		}
		logger.info("Added "+i+" traffic sensors.");
		
	
		//reading the parking sensors UUIDs and coordinates from file.
		
		reader = new BufferedReader(new InputStreamReader(MainClass.class.getResourceAsStream(Commons.AARHUS_PARKING_UUIDs)));
		int j = 0;
		
		try {
			while ((lineFormFile = reader.readLine()) != null) {
				
				if(j==0)
				{
					//break;
				}
				//parsing the UUID, lat and long 
				String UUID = lineFormFile.substring(0, lineFormFile.indexOf(","));
				String coordinates = lineFormFile.substring(lineFormFile.indexOf(",")+1, lineFormFile.length());
				String lon = coordinates.substring(0, coordinates.indexOf(" "));
				String lat = coordinates.substring(coordinates.indexOf(" ")+1, coordinates.length() );
				
				// hashmap uuid + nume (parkingGarageData)
				HashMap<String, String> inputMappingParking = new HashMap<String, String>();
				inputMappingParking.put("parkingGarageData", UUID);
				
				String eventName = getEventName(inputMappingParking);
				
				// parkingEventDetectionNode object
				ParkingEventDetectionNode parkingNode = new ParkingEventDetectionNode(j + i + "", "PublicParking", eventName,
					UUID, "0.9", "1", "30");
				
				// setting the coordinates
				Coordinate coordinate = new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat));
				parkingNode.setEventCoordinate(coordinate);

				//adding the node to ED
				eventDetection.addEventDetectionNode(inputMappingParking, parkingNode);
				//eventDetection.addEventDetectionNodeAggregated(UUID_NAME_2, parking_1_aggregated);
				
				j++;
				System.out.println("Added "+j+" parking sensors.");
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("Could not open csv file with parking sensor data");
			//e.printStackTrace();
		} catch (Exception e) {
			logger.error("Problems with the the data from the csv line "+j+". Please check if UUID is still valid");
			//e.printStackTrace();
		}
		logger.info("Added "+j+" traffic sensors.");
		
		
		//part used for injecting fake events to "events" exchange. DEMO only.
		while(Commons.fakeInjectionMode == true)
		{
			eventDetection.sendFakeData();
		}
	}
	
	private static String getEventName(HashMap<String, String> uuidHashMap) {
		
		String uuids = "";
		String eventName = "SENSOR";
		
		for(String key : uuidHashMap.keySet())
		{
			uuids = uuids + "_" +  uuidHashMap.get(key);
		}
		
		eventName = eventName + uuids;
		
		return eventName;
	}

	/**
	 * Method used to test if the Resource Management component is loaded
	 */
	private static boolean testRequestForResourceManagement() {
		
		String URLString = Commons.GET_RM_STATUS;

		URL urlToConnect = null;
		try {
			urlToConnect = new URL(URLString);
		} catch (MalformedURLException e1) {
			logger.error("Could not connect to URL: "+URLString+" because it was malformed: " +e1);
		}
		HttpURLConnection httpCon = null;
		try {
			httpCon = (HttpURLConnection) urlToConnect.openConnection();
		} catch (IOException e1) {
			logger.error("Could not open connection: "+e1);
		}
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

		try {
			httpCon.setRequestMethod("GET");
		} catch (ProtocolException e) {
			logger.error("Could not set GET protocol: ", e);
		}

		BufferedReader in;
		StringBuilder message = null;
		try {
			in = new BufferedReader(new InputStreamReader(httpCon.getInputStream(), "UTF-8"));
			String inputLine;
			message = new StringBuilder();

			while ((inputLine = in.readLine()) != null)
				message.append(inputLine);
			in.close();
		} catch (UnsupportedEncodingException e) {
			logger.error("The encoding was not supported: ", e);
		} catch (IOException e) {
			logger.error("Could not read from BufferedReader", e);
		}
		
		httpCon.disconnect();
		
		if(!message.toString().contains("true"))
		{
			return false;
		}
		return true;
	}
}
