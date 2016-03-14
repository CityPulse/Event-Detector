package com.siemens.ct.citypulse.event.detection.test;

import java.util.ArrayList;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.siemens.ct.citypulse.event.detection.api.EventDetectionNode;

import citypulse.commons.contextual_events_request.ContextualEvent;

public class AarhusPollutionEventDetectionNode extends EventDetectionNode {

	private String pollutionThresholdLow;
	private String pollutionThresholdHigh;
	
	private String nameOfStreamInEsper;
	
	public AarhusPollutionEventDetectionNode(String eventDetectionLogicID, String eventType, String eventName,
			String outputRoutingKey, String pollutionThresholdLow, String pollutionThresholdHigh) {
		super(eventDetectionLogicID, eventType, eventName, outputRoutingKey);
		this.pollutionThresholdLow = pollutionThresholdLow;
		this.pollutionThresholdHigh = pollutionThresholdHigh;
	}

	@Override
	protected ArrayList<String> getListOfInputStreamNames() {
		ArrayList<String> inputNames = new ArrayList<String>();
		inputNames.add("aarhusPollution");
		return inputNames;
	}

	@Override
	protected EPServiceProvider getEventDetectionLogic(EPServiceProvider epService) {
		
		System.out.println("\nPollution Annotated\n");
		
		nameOfStreamInEsper = getSensorStreamNameforInputName("aarhusPollution");

		
		// Low pollution
		String expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamLow") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount <= " + pollutionThresholdLow + " )";

		//System.out.println(expression);

		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				System.out.println("LOW");
			}
		});
		
		
		// Medium pollution
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamMedium") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount > " + pollutionThresholdLow
				+ " and StreetVehicleCount <= " + pollutionThresholdHigh + " )";

		//System.out.println(expression);
		
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				System.out.println("MEDIUM");
			}
		});
		
		
		// High pollution
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamHigh") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount > " + pollutionThresholdHigh + " )";

		//System.out.println(expression);

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				System.out.println("HIGH");
			}
		});
		
		
		// Low -> Medium
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamLow2Medium") + " select * from pattern [every "
				+ getUniqueStreamID("AarhusPollutionStreamLow") + " -> " + getUniqueStreamID("AarhusPollutionStreamMedium")
				+ "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("LOW->MEDIUM");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(aarhusPollution);

			}
		});
		
		
		// Low -> High
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamLow2High")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusPollutionStreamLow") + " -> "
				+ getUniqueStreamID("AarhusPollutionStreamHigh") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("Low -> High");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				sendEvent(aarhusPollution);

			}
		});
		
		// Medium -> Low
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamMedium2Low")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusPollutionStreamMedium") + " -> "
				+ getUniqueStreamID("AarhusPollutionStreamLow") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("Medium -> Low");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(aarhusPollution);

			}
		});
		
		// Medium -> High
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamMedium2High")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusPollutionStreamMedium") + " -> "
				+ getUniqueStreamID("AarhusPollutionStreamHigh") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("Medium -> High");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				sendEvent(aarhusPollution);

			}
		});
		
		
		// High -> Low
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamHigh2Low")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusPollutionStreamHigh") + " -> "
				+ getUniqueStreamID("AarhusPollutionStreamLow") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("High -> Low");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				sendEvent(aarhusPollution);

			}
		});
		
		
		// High -> Medium
		expression = "insert into " + getUniqueStreamID("AarhusPollutionStreamHigh2Medium")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusPollutionStreamHigh") + " -> "
				+ getUniqueStreamID("AarhusPollutionStreamMedium") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				System.out.println("High -> Medium");
				ContextualEvent aarhusPollution = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				sendEvent(aarhusPollution);

			}
		});

		return null;
	}

}
