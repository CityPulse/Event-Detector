package com.siemens.ct.citypulse.event.detection.api;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.log4j.Logger;

import com.espertech.esper.client.EPServiceProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.siemens.ct.citypulse.event.detection.core.Commons;

import citypulse.commons.contextual_events_request.ContextualEvent;
import citypulse.commons.data.Coordinate;

public abstract class EventDetectionNodeAggregated {

	Logger logger = Logger.getLogger(EventDetectionNodeAggregated.class);

	private String eventDetectionLogicID;
	protected String eventType;
	protected String eventName;
	//private String outputConnectionBus;
	private String outputRoutingKey;
	private Coordinate eventCoordinate;
	
	private Channel channel;
	
	// keeps the link between the name of the stream(eg: trafficDataSource) and
	// the stream's  name that is used when it was defined in Esper
	private HashMap<String, String> inputNodesDetails = new HashMap<String, String>();


	//private List<EPStatement> epnStatements = new ArrayList<EPStatement>();

	public EventDetectionNodeAggregated(String eventDetectionLogicID,
			String eventType,
			String eventName, String outputRoutingKey) {
		super();
		this.eventDetectionLogicID = eventDetectionLogicID;
		this.eventType = eventType;
		this.eventName = eventName;
		this.outputRoutingKey = outputRoutingKey;
		
	}

	protected abstract ArrayList<String> getListOfInputStreamNames();

	protected abstract EPServiceProvider getEventDetectionLogic(
			EPServiceProvider epService);
	
	protected String getSensorStreamNameforInputName(String inputName){	
		return inputNodesDetails.get(inputName);
	}
	
	protected String getUniqueStreamID(String baseStreamID){
		return baseStreamID + eventDetectionLogicID + "_AGGREGATED";
	}

	protected void deployEventDetectionLogic(EPServiceProvider epService) {
		getEventDetectionLogic(epService);	
	}

	/**
	 * Method used to publish a detected event to the message bus
	 * 
	 * @param event is a ContextualEvent object containing information of the detected event 
	 */
	protected void publishEvent(ContextualEvent event) {
		
		String RDFModel = generateRDFModel(event);

		try {

			System.out.println("Aggregated event send on \"events\" data bus for: "+event.getCeType() + " with UUID: "+outputRoutingKey);
			
			channel.exchangeDeclare(Commons.EVENTS_EXCHANGE, "topic");
			
			channel.basicPublish(Commons.EVENTS_EXCHANGE, outputRoutingKey,
					new AMQP.BasicProperties().builder().build(),
					RDFModel.getBytes());
			logger.info(event.getCeType() + " event published on bus with level: "+event.getCeLevel());
			
		} catch (IOException e) {
			logger.error("Error publishing an event on the databus for the sensor with UUID: "+outputRoutingKey, e);
		}
	}

	/**
	 * Method used to generate a RDF string for the event detected
	 * 
	 * @param event is a ContextualEvent object containing information of the detected event  
	 * @return returns a string representing the RDf equivalent of the ContextualEvent object 
	 */
	private String generateRDFModel(ContextualEvent event) {
		
		//prefixes
		final String ec = "http://purl.oclc.org/NET/UNIS/sao/ec#";
		final String sao = "http://purl.oclc.org/NET/UNIS/sao/sao#";
		final String xsn = "http://www.w3.org/2001/XMLSchema#";
		final String geo = "http://www.w3.org/2003/01/geo/wgs84_pos#";
		final String tl = "http://purl.org/NET/c4dm/timeline.owl#";
		final String prov = "http://www.w3.org/ns/prov#";
		
		final Model model = ModelFactory.createDefaultModel();
		
		//setting the prefixes
		model.setNsPrefix("ec", ec);
		model.setNsPrefix("xsd", xsn);
		model.setNsPrefix("sao", sao);
		model.setNsPrefix("geo", geo);
		model.setNsPrefix("prov", prov);
		model.setNsPrefix("tl", tl);
		
		final Resource TransportationEvent = model.createResource(ec + "TransportationEvent");
		final Resource TrafficJam = model.createResource(ec + event.getCeType());
		
		final Resource location = model.createResource();
		location.addProperty(RDF.type, model.createResource(geo + "Instant"));
		location.addLiteral(model.createProperty(geo + "lat"), event.getCeCoordinate().getLatitude())
				.addLiteral(model.createProperty(geo + "lon"), event.getCeCoordinate().getLongitude());
		
		final Property hasLocation = model.createProperty(sao + "hasLocation");
		final Property hasType = model.createProperty(sao + "hasType");
				
		final Calendar datec = Calendar.getInstance();
		datec.setTimeInMillis(event.getCeTime());
		
		datec.setTimeZone(TimeZone.getTimeZone("GMT+0"));
		final XSDDateTime xsdDate = new XSDDateTime(datec);
				
		final Resource cityEvent = model
				.createResource(sao + event.getCeID())
				.addProperty(RDF.type, TrafficJam)
				.addLiteral(model.createProperty(tl + "time"), xsdDate)
				.addLiteral(model.createProperty(sao + "hasLevel"), event.getCeLevel())
		 .addLiteral(model.createProperty(ec + "hasSource"), event.getCeName());
		
		model.add(cityEvent, hasLocation, location);
		model.add(cityEvent, hasType, TransportationEvent);
		
		final StringWriter out = new StringWriter();
		model.write(out, "N3");
		final String result = out.toString();
		
		return result;
	}

	public String getEventDetectionLogicID() {
		return eventDetectionLogicID;
	}
	
	public void sendEvent(ContextualEvent contextualEvent){
		
		publishEvent(contextualEvent);

	}

	public void setInputNodesDetails(HashMap<String, String> inputNodesDetails) {
		this.inputNodesDetails = inputNodesDetails;
	}

	public Coordinate getEventCoordinate() {
		return eventCoordinate;
	}

	public void setEventCoordinate(Coordinate eventCoordinate) {
		this.eventCoordinate = eventCoordinate;
	}

	public String getOutputRoutingKey() {
		return outputRoutingKey;
	}
	
	public void setChannel(Channel channel) {
		this.channel = channel;
	}

}
