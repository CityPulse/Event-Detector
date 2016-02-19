package com.siemens.ct.citypulse.event.detection.test;

import java.util.ArrayList;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.siemens.ct.citypulse.event.detection.api.EventDetectionNode;

import citypulse.commons.contextual_events_request.ContextualEvent;

public class TrafficJamEventDetectionNode extends EventDetectionNode {

	// the 3 parameters provided for the queries
	private String averageSpeedThreshold;
	private String streetVehicleCountThreshold;
	private String timeIntervalThreshold;

	private String nameOfStreamInEsper;

	public TrafficJamEventDetectionNode(String eventDetectionLogicID, String eventType, String eventName,
			String outputRoutingKey, String averageSpeedThreshold, String streetVehicleCountThreshold, String timeIntervalThreshold) {

		super(eventDetectionLogicID, eventType, eventName, outputRoutingKey);

		this.averageSpeedThreshold = averageSpeedThreshold;
		this.streetVehicleCountThreshold = streetVehicleCountThreshold;
		this.timeIntervalThreshold = timeIntervalThreshold;

	}

	@Override
	protected ArrayList<String> getListOfInputStreamNames() {
		ArrayList<String> inputNames = new ArrayList<String>();
		inputNames.add("trafficDataSource");
		return inputNames;
	}

	@Override
	public EPServiceProvider getEventDetectionLogic(EPServiceProvider epService) {

		System.out.println("\nTraffic Annotated\n");
		
		nameOfStreamInEsper = getSensorStreamNameforInputName("trafficDataSource");

		//CONGESTION
		String expression = "insert into " + getUniqueStreamID("TrafficCongestionStream") + " select * from "
				+ nameOfStreamInEsper + " where  (AverageSpeed <= " + averageSpeedThreshold + " and StreetVehicleCount >= "
				+ streetVehicleCountThreshold + ")";
		//System.out.println(expression);
		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				
			}
		});

		//DECONGESTION
		expression = "insert into " + getUniqueStreamID("NonTrafficCongestionStream") + " select * from "
				+ nameOfStreamInEsper + " where  (AverageSpeed > " + averageSpeedThreshold + " and StreetVehicleCount < "
				+ streetVehicleCountThreshold + ")";
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				
			}
		});

		//TRAFFIC JAM
		expression = "insert into " + getUniqueStreamID("TrafficJamStream") + " select * from pattern [every "
				+ getUniqueStreamID("TrafficCongestionStream") + " -> (timer:interval(" + timeIntervalThreshold + " sec) and not "
				+ getUniqueStreamID("NonTrafficCongestionStream") + ")]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				ContextualEvent trafficEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(trafficEvent);

			}
		});
		
		//NO TRAFFIC JAM
		expression = "insert into " + getUniqueStreamID("NormalTrafficStream") + " select * from pattern [every "
				+ getUniqueStreamID("TrafficJamStream") + " -> "
				+ getUniqueStreamID("NonTrafficCongestionStream") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				ContextualEvent trafficEvent = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(trafficEvent);

			}
		});

		return null;
	}

}
