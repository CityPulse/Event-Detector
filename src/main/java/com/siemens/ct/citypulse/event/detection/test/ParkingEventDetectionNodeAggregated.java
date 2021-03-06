package com.siemens.ct.citypulse.event.detection.test;

import java.util.ArrayList;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.siemens.ct.citypulse.event.detection.api.EventDetectionNodeAggregated;

import citypulse.commons.contextual_events_request.ContextualEvent;

public class ParkingEventDetectionNodeAggregated extends EventDetectionNodeAggregated {

	// the 3 parameters provided for the queries
	private String parkingNearlyFullThreshold;
	private String ocupancyChangeRateTreshold;
	private String parkingMonitoringInterval;

	private String nameOfStreamInEsper;

	public ParkingEventDetectionNodeAggregated(String eventDetectionLogicID, String eventType, String eventName,
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

	/**
	 * Method that contains the Esper logics for: parking full or not, and parking occupation rate ok or not
	 * 
	 * @param epService is the Esper object you want to refer to
	 */
	@Override
	public EPServiceProvider getEventDetectionLogic(EPServiceProvider epService) {

		System.out.println("\nParking Aggregated\n");
		
		nameOfStreamInEsper = getSensorStreamNameforInputName("parkingGarageData");

		// Parking nearlyFull
		String expression = "insert into " + getUniqueStreamID("ParkingGarageStatusStream") + " select * from "
				+ nameOfStreamInEsper + " where  (ParkingVehicleCount / TotalSpaces >= " + parkingNearlyFullThreshold
				+ ")";
		System.out.println(expression);
		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				ContextualEvent parkingEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(parkingEvent);
			}
		});
		
		// Parking NOT nearlyFull
		expression = "insert into " + getUniqueStreamID("ParkingGarageStatusStream") + " select * from "
				+ nameOfStreamInEsper + " where  (ParkingVehicleCount / TotalSpaces < " + parkingNearlyFullThreshold
				+ ")";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				ContextualEvent parkingEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(parkingEvent);
			}
		});
		
		// Parking occupancy rate not ok
		expression = " select * from "
				+ nameOfStreamInEsper + ".win:time(" + parkingMonitoringInterval + " sec) having  (max("
				+ nameOfStreamInEsper + ".ParkingVehicleCount) - min("
				+ nameOfStreamInEsper + ".ParkingVehicleCount)/"
				+ nameOfStreamInEsper + ".TotalSpaces) > "
				+ ocupancyChangeRateTreshold;
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				ContextualEvent parkingEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				sendEvent(parkingEvent);
			}
		});
		
		// Parking occupancy rate ok
		expression = " select * from "
				+ nameOfStreamInEsper + ".win:time(" + parkingMonitoringInterval + " sec) having  (max("
				+ nameOfStreamInEsper + ".ParkingVehicleCount) - min(" + nameOfStreamInEsper + ".ParkingVehicleCount)/"
				+ nameOfStreamInEsper + ".TotalSpaces) <= " + ocupancyChangeRateTreshold;
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				ContextualEvent parkingEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(parkingEvent);
			}
		});
		
		return null;
	}

}
