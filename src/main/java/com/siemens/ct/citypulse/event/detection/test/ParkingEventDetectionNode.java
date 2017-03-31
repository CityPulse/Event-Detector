package com.siemens.ct.citypulse.event.detection.test;

import java.util.ArrayList;
import java.util.UUID;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.siemens.ct.citypulse.event.detection.api.EventDetectionNode;

import citypulse.commons.contextual_events_request.ContextualEvent;

public class ParkingEventDetectionNode extends EventDetectionNode {

	// the 3 parameters provided for the queries
	private String parkingNearlyFullThreshold;
	private String ocupancyChangeRateTreshold;
	private String parkingMonitoringInterval;
	
	private int currentEventLevel = 0;
	private String eventID;
	
	/**
	 * Holds the ContextualEvent that will be sent for the max level
	 */
	private ContextualEvent eventToSend;

	private String nameOfStreamInEsper;

	/**
	 * Constructor method for a Parking Event Detection Node.
	 * 
	 * @param eventDetectionLogicID is the ID of your node (eg: 1 2 3 ..etc)
	 * @param eventType it's the type of node you are designing. Use a suggestive name for it eg: PublicParking or TrafficJamNode
	 * @param eventName by convention it's set to "SENSOR"
	 * @param outputRoutingKey the routing key of the stream
	 * @param parkingNearlyFullThreshold the threshold referring to the occupation percentage of a parking lot 
	 * @param ocupancyChangeRateTreshold the threshold referring to the occupation rate of occupation of a parking lot 
	 * @param parkingMonitoringInterval the threshold referring to the time interval for monitoring a parking lot 
	 */ 
	public ParkingEventDetectionNode(String eventDetectionLogicID, String eventType, String eventName,
			String outputRoutingKey, String parkingNearlyFullThreshold, String ocupancyChangeRateTreshold, String parkingMonitoringInterval) {

		super(eventDetectionLogicID, eventType, eventName, outputRoutingKey);

		this.parkingNearlyFullThreshold = parkingNearlyFullThreshold;
		this.ocupancyChangeRateTreshold = ocupancyChangeRateTreshold;
		this.parkingMonitoringInterval = parkingMonitoringInterval;

	}

	@Override
	protected ArrayList<String> getListOfInputStreamNames() {
		ArrayList<String> inputNames = new ArrayList<String>();
		inputNames.add("parkingGarageData");
		return inputNames;
	}

	@Override
	public void getEventDetectionLogic(EPServiceProvider epService) {

		//System.out.println("\nParking Annotated\n");
		
		nameOfStreamInEsper = getSensorStreamNameforInputName("parkingGarageData");

		// Parking nearlyFull
		String expression = "insert into " + getUniqueStreamID("ParkingGarageStatusStreamFull") + " select * from "
				+ nameOfStreamInEsper + " where  (ParkingVehicleCount / TotalSpaces >= " + parkingNearlyFullThreshold
				+ ")";
		
		//System.out.println(expression);
		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				
				// if this is the start of a traffic jam incident (currentEventLevel is equal to 0), then set a eventID
				// this event ID will remain the same until the end of the traffic jam event (currentEventLevel becomes 0 again)
				if(currentEventLevel==0)
				{
					eventID = UUID.randomUUID().toString();
				}
				
				currentEventLevel = 1;
				
				eventToSend = new ContextualEvent(eventID, eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(eventToSend);
			}
		});
		
		// Parking NOT nearlyFull temporary
		expression = "insert into " + getUniqueStreamID("ParkingGarageStatusStreamEmptyTemp") + " select * from "
				+ nameOfStreamInEsper + " where  (ParkingVehicleCount / TotalSpaces < " + parkingNearlyFullThreshold
				+ ")";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				
				
			}
		});
		
		// Parking NOT nearlyFull
		expression = "insert into " + getUniqueStreamID("ParkingGarageStatusStreamEmpty") + " select * from pattern [every "
				+ getUniqueStreamID("ParkingGarageStatusStreamFull") + " -> " + getUniqueStreamID("ParkingGarageStatusStreamEmptyTemp")
				+ "]";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				
				currentEventLevel = 0;
				
				eventToSend = new ContextualEvent(eventID, eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(eventToSend);
			}
		});
		
		
		
		/*
		// Parking occupancy rate not ok
		expression = "insert into "+getUniqueStreamID("ParkingGarageOccupancyRateNOTOk")+" select * from "
				+ nameOfStreamInEsper + ".win:time(" + parkingMonitoringInterval + " sec) having  (max("
				+ nameOfStreamInEsper + ".ParkingVehicleCount) - min("
				+ nameOfStreamInEsper + ".ParkingVehicleCount)/"
				+ nameOfStreamInEsper + ".TotalSpaces) > "
				+ ocupancyChangeRateTreshold;
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				//System.out.println("Level2_2");
			}
		});
		
		// Parking occupancy rate ok
		expression = "insert into "+getUniqueStreamID("ParkingGarageOccupancyRateOk")+" select * from "
				+ nameOfStreamInEsper + ".win:time(" + parkingMonitoringInterval + " sec) having  (max("
				+ nameOfStreamInEsper + ".ParkingVehicleCount) - min(" + nameOfStreamInEsper + ".ParkingVehicleCount)/"
				+ nameOfStreamInEsper + ".TotalSpaces) <= " + ocupancyChangeRateTreshold;
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				//System.out.println("Level0_2");
			}
		});
		
		
		//====================================
		// Max event level logics
		//====================================
				

		// Max event level logics: 1_0 -> 2_2
		expression = "insert into " + getUniqueStreamID("ParkingGarageLevel2") + " select * from pattern [every " + getUniqueStreamID("ParkingGarageStatusStreamEmpty")
				+ " -> (timer:interval(10 sec) and " + getUniqueStreamID("ParkingGarageOccupancyRateNOTOk") + ")] ";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				eventToSend = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				sendEvent(eventToSend);
			}
		});
		
		// Max event level logics: 1_1 -> 2_2
		expression = "insert into " + getUniqueStreamID("ParkingGarageLevel2") + " select * from pattern [every " + getUniqueStreamID("ParkingGarageStatusStreamFull")
				+ " ->(timer:interval(10 sec) and " + getUniqueStreamID("ParkingGarageOccupancyRateNOTOk") + ")] ";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				eventToSend = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				sendEvent(eventToSend);
			}
		});
		
		// Max event level logics: 2_0 -> 1_1
		expression = "insert into " + getUniqueStreamID("ParkingGarageLevel1") + " select * from pattern [every "
				+ getUniqueStreamID("ParkingGarageOccupancyRateOk") + " ->(timer:interval(10 sec) and "
				+ getUniqueStreamID("ParkingGarageStatusStreamFull") + ")] ";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				eventToSend = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(eventToSend);
			}
		});
		
		// Max event level logics
		expression = "insert into " + getUniqueStreamID("ParkingGarageLevel0") + " select * from pattern [every " + getUniqueStreamID("ParkingGarageStatusStreamEmpty")
				+ " ->(timer:interval(10 sec) and not " + getUniqueStreamID("ParkingGarageOccupancyRateNOTOk")+ ") or "+getUniqueStreamID("ParkingGarageOccupancyRateOk")+" ->(timer:interval(10 sec) and not " + getUniqueStreamID("ParkingGarageOccupancyRateNOTOk")+ ")] ";
		
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				eventToSend = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(eventToSend);
			}
		});*/
			
	}

}
