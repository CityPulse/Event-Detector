package com.siemens.ct.citypulse.event.detection.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.siemens.ct.citypulse.event.detection.api.EventDetection;
import com.siemens.ct.citypulse.event.detection.core.Commons;

import citypulse.commons.data.Coordinate;

public class MainClass {
	
	static Logger logger = Logger.getLogger(MainClass.class);

	public static void main(String[] args) {		
		EventDetection eventDetection = new EventDetection();
				
		//reading the UUIDs from file
		String line2;
		int i=0;

		//reading the traffic sensors UUIDs and coordinates from file.
		BufferedReader reader = new BufferedReader(new InputStreamReader(MainClass.class.getResourceAsStream(Commons.AARHUS_TRFFIC_UUIDs)));
		
		try {
			while ((line2 = reader.readLine()) != null) {
					
				if(i==30)
				{
					break;
				}
				//parsing the UUID, lat and long 
				String UUID = line2.substring(0, line2.indexOf(","));
				
				//this sensor has some problems in SPARQL endpoint
				if(!UUID.equals("847938fe-5d03-5c36-913d-31e4d2b082b5"))
				{
					//getting coordinates
					String coordinates = line2.substring(line2.indexOf(",")+1, line2.length());
					String lon = coordinates.substring(0, coordinates.indexOf(" "));
					String lat = coordinates.substring(coordinates.indexOf(" ")+1, coordinates.length() );
					
					
					// trafficJamEventDetectionNode object
					TrafficJamEventDetectionNode trafficJamNode_1 = new TrafficJamEventDetectionNode(i + "", "TrafficJam",
							"SENSOR", UUID, "35", "10", "24");
					
					//TrafficJamEventDetectionNodeAggregated trafficJamNode_1_aggregated = new TrafficJamEventDetectionNodeAggregated(i + "", "TrafficJam",
						//	"SENSOR", UUID, "35", "10", "24");
					
					//setting the coordinates
					Coordinate coordinate = new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat));
					trafficJamNode_1.setEventCoordinate(coordinate);
					
					//trafficJamNode_1_aggregated.setEventCoordinate(coordinate);

					// eventDetectionLogicInputMapping hashmap nume (trafficDataSource) + UUID
					HashMap<String, String> UUID_NAME_1 = new HashMap<String, String>();
					UUID_NAME_1.put("trafficDataSource", UUID);

					//adding the node to ED
					eventDetection.addEventDetectionNode(UUID_NAME_1, trafficJamNode_1);
					//eventDetection.addEventDetectionNodeAggregated(UUID_NAME_1, trafficJamNode_1_aggregated);
					
					i++;
					System.out.println("Added "+i+" traffic sensors.");
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Added "+i+" traffic sensors.");
		
	
		//reading the parking sensors UUIDs and coordinates from file.
		
		reader = new BufferedReader(new InputStreamReader(MainClass.class.getResourceAsStream(Commons.AARHUS_PARKING_UUIDs)));
		int j = 0;
		
		try {
			while ((line2 = reader.readLine()) != null) {
				
				//parsing the UUID, lat and long 
				String UUID = line2.substring(0, line2.indexOf(","));
				String coordinates = line2.substring(line2.indexOf(",")+1, line2.length());
				String lon = coordinates.substring(0, coordinates.indexOf(" "));
				String lat = coordinates.substring(coordinates.indexOf(" ")+1, coordinates.length() );
				
				// parkingEventDetectionNode object
				ParkingEventDetectionNode parking_1 = new ParkingEventDetectionNode(j + i + "", "PublicParking", "SENSOR",
					UUID, "0.9", "1", "30");
				
				//ParkingEventDetectionNodeAggregated parking_1_aggregated = new ParkingEventDetectionNodeAggregated(j + i + "", "PublicParking", "SENSOR",
					//	UUID, "0.9", "1", "30");
				
				// setting the coordinates
				Coordinate coordinate = new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat));
				parking_1.setEventCoordinate(coordinate);
				
				//parking_1_aggregated.setEventCoordinate(coordinate);

				// hashmap uuid + nume (parkingGarageData)
				HashMap<String, String> UUID_NAME_2 = new HashMap<String, String>();
				UUID_NAME_2.put("parkingGarageData", UUID);

				//adding the node to ED
				eventDetection.addEventDetectionNode(UUID_NAME_2, parking_1);
				//eventDetection.addEventDetectionNodeAggregated(UUID_NAME_2, parking_1_aggregated);
				
				j++;
				System.out.println("Added "+j+" parking sensors.");
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Added "+j+" traffic sensors.");
		
		
		//part used for injecting fake events to "events" exchange. Demo only.
		while(Commons.testParameter == true)
		{
			eventDetection.sendFakeData();
		}
	}
}
