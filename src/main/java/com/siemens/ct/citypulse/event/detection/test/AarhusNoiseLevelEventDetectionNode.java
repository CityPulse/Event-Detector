package com.siemens.ct.citypulse.event.detection.test;

import java.util.ArrayList;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.siemens.ct.citypulse.event.detection.api.EventDetectionNode;

import citypulse.commons.contextual_events_request.ContextualEvent;

public class AarhusNoiseLevelEventDetectionNode extends EventDetectionNode {

	private String noiseLevelThresholdLow;
	private String noiseLevelThresholdHigh;
	
	private String nameOfStreamInEsper;
	
	public AarhusNoiseLevelEventDetectionNode(String eventDetectionLogicID, String eventType, String eventName,
			String outputRoutingKey, String noiseLevelThresholdLow, String noiseLevelThresholdHigh) {
		super(eventDetectionLogicID, eventType, eventName, outputRoutingKey);
		this.noiseLevelThresholdLow = noiseLevelThresholdLow;
		this.noiseLevelThresholdHigh = noiseLevelThresholdHigh;
	}

	@Override
	protected ArrayList<String> getListOfInputStreamNames() {
		ArrayList<String> inputNames = new ArrayList<String>();
		inputNames.add("aarhusNoiseLevel");
		return inputNames;
	}

	@Override
	protected void getEventDetectionLogic(EPServiceProvider epService) {
		
		//System.out.println("Noise Annotated\n");
		
		nameOfStreamInEsper = getSensorStreamNameforInputName("aarhusNoiseLevel");

		
		// Low pollution
		String expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamLow") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount*AverageSpeed <= " + noiseLevelThresholdLow + " )";

		//System.out.println(expression);

		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				//System.out.println("LOW NOISE for: "+nameOfStreamInEsper);
			}
		});
		
		
		// Medium pollution
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamMedium") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount*AverageSpeed > " + noiseLevelThresholdLow
				+ " and StreetVehicleCount*AverageSpeed <= " + noiseLevelThresholdHigh + " )";

		//System.out.println(expression);
		
		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				//System.out.println("MEDIUM NOISE for: "+nameOfStreamInEsper);
			}
		});
		
		
		// High pollution
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamHigh") + " select * from "
				+ nameOfStreamInEsper + " where  (StreetVehicleCount*AverageSpeed > " + noiseLevelThresholdHigh + " )";

		//System.out.println(expression);

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {
				//System.out.println("HIGH NOISE for: "+nameOfStreamInEsper);
			}
		});
		
		
		// Low -> Medium
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamLow2Medium") + " select * from pattern [every "
				+ getUniqueStreamID("AarhusNoiseLevelStreamLow") + " -> " + getUniqueStreamID("AarhusNoiseLevelStreamMedium")
				+ "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("LOW->MEDIUM NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				//System.out.println("LOW->MEDIUM NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});
		
		
		// Low -> High
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamLow2High")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusNoiseLevelStreamLow") + " -> "
				+ getUniqueStreamID("AarhusNoiseLevelStreamHigh") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("Low -> High NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				//System.out.println("LOW->HIGH NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});
		
		// Medium -> Low
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamMedium2Low")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusNoiseLevelStreamMedium") + " -> "
				+ getUniqueStreamID("AarhusNoiseLevelStreamLow") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("Medium -> Low NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				//System.out.println("MEDIUM->LOW NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});
		
		// Medium -> High
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamMedium2High")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusNoiseLevelStreamMedium") + " -> "
				+ getUniqueStreamID("AarhusNoiseLevelStreamHigh") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("Medium -> High NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 2);
				//System.out.println("MEDIUM->HIGH NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});
		
		
		// High -> Low
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamHigh2Low")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusNoiseLevelStreamHigh") + " -> "
				+ getUniqueStreamID("AarhusNoiseLevelStreamLow") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("High -> Low NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 0);
				//System.out.println("HIGH->LOW NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});
		
		
		// High -> Medium
		expression = "insert into " + getUniqueStreamID("AarhusNoiseLevelStreamHigh2Medium")
				+ " select * from pattern [every " + getUniqueStreamID("AarhusNoiseLevelStreamHigh") + " -> "
				+ getUniqueStreamID("AarhusNoiseLevelStreamMedium") + "]";

		statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new UpdateListener() {

			public void update(EventBean[] arg0, EventBean[] arg1) {

				//System.out.println("High -> Medium NOISE");
				ContextualEvent aarhusNoiseLevel = new ContextualEvent(eventType, eventName, System.currentTimeMillis(),
						getEventCoordinate(), 1);
				//System.out.println("HIGH->MEDIUM NOISE for: "+nameOfStreamInEsper);
				sendEvent(aarhusNoiseLevel);

			}
		});

	}
	
	
}
