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

public abstract class EventDetectionNode {

	Logger logger = Logger.getLogger(EventDetectionNode.class);

	/**
	 * The ID of the stream that was added in the MainClass.class
	 */
	private String eventDetectionLogicID;
	
	/**
	 * The type of event. Eg: TrafficJam , PublicParking
	 */
	protected String eventType;
	
	/**
	 * The name of the event. It was discusses upon the string "SENSOR". 
	 * But it may be changed to the UUID of the sensor
	 */
	protected String eventName;
	
	/**
	 * The UUID of the stream
	 */
	private String outputRoutingKey;
	
	/**
	 * The Coordinate object that describes the sensor's location
	 */
	private Coordinate eventCoordinate;
	
	/**
	 * The channel that will be used to publish on the events
	 */
	private Channel channel;
	
	/**
	 * HaspMap that keeps the link between the name of the stream(eg: trafficDataSource) and
	 * the stream's  name that is used when it was defined in Esper
	 */
	private HashMap<String, String> inputNodesDetails = new HashMap<String, String>();


	//private List<EPStatement> epnStatements = new ArrayList<EPStatement>();

	/**
	 * Constructor method for an Event Detection Node.
	 * 
	 * @param eventDetectionLogicID is the ID of your node (eg: 1 2 3 ..etc)
	 * @param eventType it's the type of node you are designing. Use a suggestive name for it eg: PublicParking or TrafficJamNode
	 * @param eventName by convention it's set to "SENSOR"
	 * @param outputRoutingKey the UUID of the stream
	 */
	public EventDetectionNode(String eventDetectionLogicID,
			String eventType,
			String eventName, String outputRoutingKey) {
		super();
		this.eventDetectionLogicID = eventDetectionLogicID;
		this.eventType = eventType;
		this.eventName = eventName;
		this.outputRoutingKey = outputRoutingKey;
		
	}

	protected abstract ArrayList<String> getListOfInputStreamNames();

	/**
	 * Method used to start the nodes logic
	 * 
	 * @param epService the Esper object used
	 * @return no return type
	 */
	protected abstract EPServiceProvider getEventDetectionLogic(
			EPServiceProvider epService);
	
	
	protected String getSensorStreamNameforInputName(String inputName){	
		return inputNodesDetails.get(inputName);
	}
	
	/**
	 * Method that generates a unique stream ID to be used in Esper queries
	 * 
	 * @param baseStreamID
	 *            the name of the stream you plan on using as a base in Esper
	 *            queries eg: ParkingGarageStatusStream
	 * @return a string formad by concatenating the baseStreamID, the
	 *         eventDetectionLogicID and "_ANNOTATED" for annotated streams and
	 *         "_AGGREGATED" for aggregated streams
	 */
	protected String getUniqueStreamID(String baseStreamID){
		return baseStreamID + eventDetectionLogicID + "_ANNOTATED";
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
		
		/**
		 * The RDF message that was generated starting from the ContextualEvent received.
		 */
		String RDFModel = generateRDFModel(event);
		
		/*System.out.println();
		System.out.println(event);
		System.out.println();*/

		try {
			
			System.out.println("Annotated event send on \"events\" data bus for: "+event.getCeType() + " with UUID: "+outputRoutingKey);
			
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

	/**
	 * Method used to get the eventDetectionLogicID
	 * 
	 * @return eventDetectionLogicID
	 */
	public String getEventDetectionLogicID() {
		return eventDetectionLogicID;
	}
	
	/**
	 * Method used to send the newly detected event to GDI
	 * 
	 * @param contextualEvent the object that holds the information you want to send to GDI
	 */
	protected void sendEvent(ContextualEvent contextualEvent){
		
		publishEvent(contextualEvent);

	}

	public void setInputNodesDetails(HashMap<String, String> inputNodesDetails) {
		this.inputNodesDetails = inputNodesDetails;
	}

	public Coordinate getEventCoordinate() {
		return eventCoordinate;
	}

	/**
	 * Method to set the Coordinates of the stream you want to register
	 * 
	 * @param eventCoordinate the Coordinate object
	 */
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
