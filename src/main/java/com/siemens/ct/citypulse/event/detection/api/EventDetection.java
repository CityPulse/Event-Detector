package com.siemens.ct.citypulse.event.detection.api;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.siemens.ct.citypulse.event.detection.core.Commons;
import com.siemens.ct.citypulse.event.detection.core.DataBusConnector;
import com.siemens.ct.citypulse.event.detection.resources.StreamDetails;

import citypulse.commons.contextual_events_request.ContextualEvent;
import citypulse.commons.data.Coordinate;
import eu.citypulse.uaso.gdiclient.CpGdiInterface;

/**
 * @author Serbanescu Bogdan Victor
 *
 */
public class EventDetection {

	private Logger logger = Logger.getLogger(EventDetection.class);

	private EPServiceProvider epService;

	private String dataBusIP;

	private int dataBusPort;

	private Channel dataBusChannel;

	private CpGdiInterface cp_GDI_Interface;

	// key = the nodeID, and value = input hashMap (key = stream name, and value = UUID of stream)
	private HashMap<String, HashMap<String, String>> deployedEventDetectionNodeConnections = new HashMap<String, HashMap<String, String>>();
	
	// key = the nodeID, and value = input hashMap (key = stream name, and value = UUID of stream). FOR AGGREGATED
	private HashMap<String, HashMap<String, String>> deployedEventDetectionNodeConnectionsAggregated = new HashMap<String, HashMap<String, String>>();

	// keeps track of the event detection modules which are deployed
	private HashMap<String, EventDetectionNode> deployedEventDetectionLogic = new HashMap<String, EventDetectionNode>();
	
	// keeps track of the event detection modules which are deployed. FOR AGGREGATED
	private HashMap<String, EventDetectionNodeAggregated> deployedEventDetectionLogicAggregated = new HashMap<String, EventDetectionNodeAggregated>();

	// the key represents the ID of the stream
	// the value is represented by a list of EventDetectionLogicModules which
	// are connected to the stream
	private HashMap<String, ArrayList<String>> streamBindingToEventDetectionLogicModules = new HashMap<String, ArrayList<String>>();
	
	// the key represents the ID of the stream
	// the value is represented by a list of EventDetectionLogicModules which
	// are connected to the stream. FOR AGGREGATED
	private HashMap<String, ArrayList<String>> streamBindingToEventDetectionLogicModulesAggregated = new HashMap<String, ArrayList<String>>();

	// keeps the linking between routingKeys and instances of DataBusConnector
	// key = routingKey, and value = instance of DataBusConnector
	private HashMap<String, DataBusConnector> dataBusConnectorLinker = new HashMap<String, DataBusConnector>();
	
	// keeps the linking between routingKeys and instances of DataBusConnector
	// key = routingKey, and value = instance of DataBusConnector. FOR AGGREGATED
	private HashMap<String, DataBusConnector> dataBusConnectorLinkerAggregated = new HashMap<String, DataBusConnector>();

	// keeps the link between the name of the stream(eg: trafficDataSource) and
	// the stream's name that is used when it was defined in
	// Esper
	private HashMap<String, String> InputNodesDetails = new HashMap<String, String>();
	
	// keeps the link between the name of the stream(eg: trafficDataSource) and
	// the stream's name that is used when it was defined in
	// Esper. FOR AGGREGATED
	private HashMap<String, String> InputNodesDetailsAggregated = new HashMap<String, String>();

	// HashMap that links the UUID and the name of the stream in Esper
	private HashMap<String, String> UUIDAndEsperNameLinker = new HashMap<String, String>();
	
	// HashMap that links the UUID and the name of the stream in Esper. FOR AGGREGATED
	private HashMap<String, String> UUIDAndEsperNameLinkerAggregated = new HashMap<String, String>();

	// holds all the IDs of the nodes added
	private ArrayList<String> listOfNodeIDs = new ArrayList<String>();
	
	// holds all the IDs of the aggregated nodes added. FOR AGGREGATED
	private ArrayList<String> listOfNodeIDsAggregated = new ArrayList<String>();
	
	//parameters used to set-up the AMQP bus to publish events on to GDI
	public static Connection transmisingConnection;
	public static Channel transmisingChannel;
	public static ConnectionFactory factory;
	

	/**
	 * Method used to initialize the CEP engine, connections to the data bus and GDI
	 * 
	 */
	public EventDetection() {
		startCEPEngine();
		connectToMessageBus();
		connectToDataBusForGDI();
		initializeGDI();
	}

	/**
	 * Method used to inject events in the "events" exchange for demo testing
	 * 
	 */
	public void sendFakeData() {		
		
		try {
			
			dataBusChannel.exchangeDeclare(Commons.EVENTS_EXCHANGE, "topic");
			
			System.out.println("Published fake data - traffic");
			
			ContextualEvent trafficEvent = new ContextualEvent("TrafficJam", "SENSOR", System.currentTimeMillis(),
					new Coordinate(10.218501513474507,56.19013109480471), 1);
			String RDFModelTraffic =  generateRDFModel(trafficEvent);
			
			dataBusChannel.basicPublish(Commons.EVENTS_EXCHANGE, "4a838c4b-30d0-5fb4-b3b5-16d6c5c4ff9f",
					new AMQP.BasicProperties().builder().build(),
					RDFModelTraffic.getBytes());
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				logger.error("The thread was interrupted while in event injecting sention. " , e);
			}
			
			System.out.println("Published fake data - parking");
			
			ContextualEvent parkingEvent = new ContextualEvent("PublicParking", "SENSOR", System.currentTimeMillis(),
					new Coordinate(10.21284,56.16184), 2);
			String RDFModelParking =  generateRDFModel(parkingEvent);
			
			dataBusChannel.basicPublish(Commons.EVENTS_EXCHANGE, "80043a6c-3bdb-5e47-9199-151fb8cfd619",
					new AMQP.BasicProperties().builder().build(),
					RDFModelParking.getBytes());
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				logger.error("The thread was interrupted while in event injecting sention. " , e);
			}
						
		} catch (IOException e) {
			logger.error("Problems found in creating the fake RDF's that were to be sent on 'events' exchange. " , e);
		}
		
	}

	/**
	 * Method used to generate the appropriate RDF to be sent to "events" exchange
	 * 
	 * @param parkingEvent the ContextualEvent object that is used
	 * @return a string representing the RDF that will be sent on the "events" exchange
	 */
	private String generateRDFModel(ContextualEvent parkingEvent) {

		final String ec = "http://purl.oclc.org/NET/UNIS/sao/ec#";
		final String sao = "http://purl.oclc.org/NET/UNIS/sao/sao#";
		final String xsn = "http://www.w3.org/2001/XMLSchema#";
		final String geo = "http://www.w3.org/2003/01/geo/wgs84_pos#";
		final String tl = "http://purl.org/NET/c4dm/timeline.owl#";
		final String prov = "http://www.w3.org/ns/prov#";

		final Model model = ModelFactory.createDefaultModel();

		// setting the prefixes
		model.setNsPrefix("ec", ec);
		model.setNsPrefix("xsd", xsn);
		model.setNsPrefix("sao", sao);
		model.setNsPrefix("geo", geo);
		model.setNsPrefix("prov", prov);
		model.setNsPrefix("tl", tl);

		final Resource TransportationEvent = model.createResource(ec + "TransportationEvent");
		final Resource TrafficJam = model.createResource(ec + parkingEvent.getCeType());

		final Resource location = model.createResource();
		location.addProperty(RDF.type, model.createResource(geo + "Instant"));
		location.addLiteral(model.createProperty(geo + "lat"), parkingEvent.getCeCoordinate().getLatitude())
				.addLiteral(model.createProperty(geo + "lon"), parkingEvent.getCeCoordinate().getLongitude());

		final Property hasLocation = model.createProperty(sao + "hasLocation");
		final Property hasType = model.createProperty(sao + "hasType");

		final Calendar datec = Calendar.getInstance();
		datec.setTimeInMillis(parkingEvent.getCeTime());

		datec.setTimeZone(TimeZone.getTimeZone("GMT+0"));
		final XSDDateTime xsdDate = new XSDDateTime(datec);

		final Resource cityEvent = model.createResource(sao + parkingEvent.getCeID()).addProperty(RDF.type, TrafficJam)
				.addLiteral(model.createProperty(tl + "time"), xsdDate)
				.addLiteral(model.createProperty(sao + "hasLevel"), parkingEvent.getCeLevel())
				.addLiteral(model.createProperty(ec + "hasSource"), parkingEvent.getCeName());

		model.add(cityEvent, hasLocation, location);
		model.add(cityEvent, hasType, TransportationEvent);

		final StringWriter out = new StringWriter();
		model.write(out, "N3");
		final String result = out.toString();

		return result;
	}

	/**
	 * Method used to establish a connection to the AMQP message bus that will be used to send detected events to GDI
	 * 
	 */
	private void connectToDataBusForGDI() {
		
		try {
			factory = new ConnectionFactory();
			try {
				factory.setUri(Commons.GDI_AMQP_URI);
			} catch (KeyManagementException e) {
				logger.error("Problems connecting to URI: amqp://guest:guest@131.227.92.55:8007. " , e);
			} catch (NoSuchAlgorithmException e) {
				logger.error("Problems connecting to URI: amqp://guest:guest@131.227.92.55:8007. " , e);
			} catch (URISyntaxException e) {
				logger.error("Problems connecting to URI: amqp://guest:guest@131.227.92.55:8007. " , e);
			}

			transmisingConnection = factory.newConnection();
			transmisingChannel = transmisingConnection.createChannel();
			
		} catch (IOException e) {
			logger.error("Problems creating new connection factory for GDI connection via AMQP. " , e);
		} 		
		
	}

	/**
	 * Method in which we initialize the GDI and remove all the existing EventStreams
	 * 
	 */
	private void initializeGDI() {
		try {
			System.out.println("GDI");
			cp_GDI_Interface = new CpGdiInterface(Commons.GDIhostname, Commons.GDIport);
			cp_GDI_Interface.removeAllEventStream();
		} catch (SQLException e) {
			logger.error("Error trying to remove all event streams from GDI. ", e);
		} catch (ClassNotFoundException e) {
			logger.error("Error trying to connect to GDI on localhost: "+5348 , e);
		}
	}

	/**
	 * Method in which we start the CEP engine
	 * 
	 */
	private void startCEPEngine() {
		Configuration cepConfig = new Configuration();
		this.epService = EPServiceProviderManager.getProvider("CityPulse Event Detection engine", cepConfig);
		logger.info("EventDetection: CEP engine started.");

	}

	/**
	 * Method in which we create a connection to the message bus
	 * 
	 */
	private void connectToMessageBus() {

		dataBusIP = Commons.dataBusIP;
		dataBusPort = Integer.parseInt(Commons.dataBusPort);

		ConnectionFactory factory = new ConnectionFactory();

		factory.setHost(dataBusIP);
		factory.setPort(dataBusPort);

		try {
			Connection connection = factory.newConnection();
			dataBusChannel = connection.createChannel();

		} catch (IOException e) {
			logger.error("EventDetection: Cannot connect to the data bus: " + dataBusIP + ", on port: " + dataBusPort,e);
		}
	}
	
	/**
	 * Complex method that checks if the node we want to add was added before, if not it will check to see if its streams exist in inputMap
	 * if they do, we put the new node in a HashMap with the key - it's logic ID and value - the new node.
	 * We then pass through each UUID described in the inputMapping: if the stream is already used by another node, we update the HashMap with key - the stream UUID
	 * and value - the list of logic IDs of nodes that use that particular stream, if not, we use the method connectNewStreamToTheEventDetection 
	 * to connect the new stream.
	 * After the stream is connected, we add our node to deployed nodes HashMap with the key - the node logic ID and value - it's inputMapping
	 * For every inputStream we set the InputNodesDetails HashMap with the key - the UUID of the stream and value - the name of the stream in Esper
	 * The node is then added to the list of already deployed nodes. And deployed afterwards.
	 * We then register the eventStream to GDI.
	 *
	 * @param eventDetectionLogicInputMaping is a HashMap linking the inputStreamName and it's UUID
	 * @param eventDetectionNode is the EventDetection object created in MainClass
	 */
	public void addEventDetectionNode(HashMap<String, String> eventDetectionLogicInputMaping,
			EventDetectionNode eventDetectionNode) {

		// checks if the node we want to add was added before
		if (!listOfNodeIDs.contains(eventDetectionNode.getEventDetectionLogicID())) {
			
			// test if the eventDetectionLogicInputMaping is complete (method in ParkingEventdetectionNode/TrafficJamEventdetectionNode.java class)
			ArrayList<String> inputStreamNames = eventDetectionNode.getListOfInputStreamNames();

			for (String key : inputStreamNames) {
				if (!eventDetectionLogicInputMaping.containsKey(key)) {
					System.out.println("B");
					logger.info("Error while deploying event detection logic with the name "
							+ eventDetectionNode.getEventDetectionLogicID());
					logger.info("      The input map must contain the following keys: " + inputStreamNames);
					logger.info("      The input map contains the following pairs of (key:stram UUIDS): "
							+ eventDetectionLogicInputMaping);
					logger.info("      The deployment process was stopped.");

					return;
				}
			}

			// register the event detection logic
			deployedEventDetectionLogic.put(eventDetectionNode.getEventDetectionLogicID(), eventDetectionNode);

			// test if the requires streams are connected to the EventDetection
			ArrayList<String> list;

			// We then pass through each UUID described in the inputMapping: if
			// the stream is already used by another node, we update the HashMap
			// with key - the stream UUID and value - the list of logic IDs of nodes that use that particular stream
			for (String inputStreamUUID : eventDetectionLogicInputMaping.values()) {

				if (streamBindingToEventDetectionLogicModules.containsKey(inputStreamUUID)) {
					list = streamBindingToEventDetectionLogicModules.get(inputStreamUUID);
					list.add(eventDetectionNode.getEventDetectionLogicID());

				} else {

					list = new ArrayList<String>();
					list.add(eventDetectionNode.getEventDetectionLogicID());

					streamBindingToEventDetectionLogicModules.put(inputStreamUUID, list);
					
					//we use the method connectNewStreamToTheEventDetection to connect the new stream
					connectNewStreamToTheEventDetection(inputStreamUUID,Commons.ANNOTATED_DATA_EXCHANGE);

				}
			}

			// add our node to deployed nodes HashMap with the key - the node
			// logic ID and value - it's inputMapping
			deployedEventDetectionNodeConnections.put(eventDetectionNode.getEventDetectionLogicID(),
					eventDetectionLogicInputMaping);

			// for every inputStream we set the InputNodesDetails HashMap with
			// the key - the UUID of the stream and value - the name of the
			// stream in Esper
			for (String key : inputStreamNames) {
				InputNodesDetails.put(key, UUIDAndEsperNameLinker.get(eventDetectionLogicInputMaping.get(key)));
			}

			eventDetectionNode.setInputNodesDetails(InputNodesDetails);
			
			//setting the channel on which the detected events will be sent to GDI
			eventDetectionNode.setChannel(transmisingChannel);

			// and deployed
			eventDetectionNode.deployEventDetectionLogic(epService);
			
			// node is then added to the list of already deployed nodes
			listOfNodeIDs.add(eventDetectionNode.getEventDetectionLogicID());

			//adding a new eventStream to GDI
			addNewEventStreamToGDI(eventDetectionNode);
			
		}

	}

	/**
	 * Complex method that checks if the node we want to add was added before, if not it will check to see if its streams exist in inputMap
	 * if they do, we put the new node in a HashMap with the key - it's logic ID and value - the new node.
	 * We then pass through each UUID described in the inputMapping: if the stream is already used by another node, we update the HashMap with key - the stream UUID
	 * and value - the list of logic IDs of nodes that use that particular stream, if not, we use the method connectNewStreamToTheEventDetection 
	 * to connect the new stream.
	 * After the stream is connected, we add our node to deployed nodes HashMap with the key - the node logic ID and value - it's inputMapping
	 * For every inputStream we set the InputNodesDetails HashMap with the key - the UUID of the stream and value - the name of the stream in Esper
	 * The node is then added to the list of already deployed nodes. And deployed afterwards.
	 * We then register the eventStream to GDI.
	 * 
	 * @param eventDetectionLogicInputMaping is a HashMap linking the inputStreamName and it's UUID
	 * @param eventDetectionNodeAggregated is the EventDetection object for aggregated created in MainClass
	 */
	public void addEventDetectionNodeAggregated(HashMap<String, String> eventDetectionLogicInputMaping,
			EventDetectionNodeAggregated eventDetectionNodeAggregated) {
		
		// checks if the node we want to add was added before
		if (!listOfNodeIDsAggregated.contains(eventDetectionNodeAggregated.getEventDetectionLogicID())) {
			
			// test if the eventDetectionLogicInputMaping is complete (method in ParkingEventdetectionNode/TrafficJamEventdetectionNode.java class)
			ArrayList<String> inputStreamNames = eventDetectionNodeAggregated.getListOfInputStreamNames();

			//System.out.println("A  ##  Aggregated");
			for (String key : inputStreamNames) {
				if (!eventDetectionLogicInputMaping.containsKey(key)) {
					System.out.println("B");
					logger.info("Error while deploying event detection logic with the name "
							+ eventDetectionNodeAggregated.getEventDetectionLogicID());
					logger.info("      The input map must contain the following keys: " + inputStreamNames);
					logger.info("      The input map contains the following pairs of (key:stram UUIDS): "
							+ eventDetectionLogicInputMaping);
					logger.info("      The deployment process was stopped.");

					return;
				}
			}

			// register the event detection logic
			deployedEventDetectionLogicAggregated.put(eventDetectionNodeAggregated.getEventDetectionLogicID(), eventDetectionNodeAggregated);

			// test if the requires streams are connected to the EventDetection
			ArrayList<String> list;

			// We then pass through each UUID described in the inputMapping: if
			// the stream is already used by another node, we update the HashMap
			// with key - the stream UUID and value - the list of logic IDs of nodes that use that particular stream
			for (String inputStreamUUID : eventDetectionLogicInputMaping.values()) {

				if (streamBindingToEventDetectionLogicModulesAggregated.containsKey(inputStreamUUID)) {
					list = streamBindingToEventDetectionLogicModulesAggregated.get(inputStreamUUID);
					list.add(eventDetectionNodeAggregated.getEventDetectionLogicID());

				} else {

					list = new ArrayList<String>();
					list.add(eventDetectionNodeAggregated.getEventDetectionLogicID());

					streamBindingToEventDetectionLogicModulesAggregated.put(inputStreamUUID, list);
					
					//we use the method connectNewStreamToTheEventDetection to connect the new stream		
					connectNewStreamToTheEventDetection(inputStreamUUID,Commons.AGGREGATED_DATA_EXCHANGE);

				}
			}

			// add our node to deployed nodes HashMap with the key - the node
			// logic ID and value - it's inputMapping
			deployedEventDetectionNodeConnectionsAggregated.put(eventDetectionNodeAggregated.getEventDetectionLogicID(),
					eventDetectionLogicInputMaping);

			// for every inputStream we set the InputNodesDetails HashMap with
			// the key - the UUID of the stream and value - the name of the
			// stream in Esper
			for (String key : inputStreamNames) {
				InputNodesDetailsAggregated.put(key, UUIDAndEsperNameLinkerAggregated.get(eventDetectionLogicInputMaping.get(key)));
				//System.out.println("# "+InputNodesDetails);
			}

			eventDetectionNodeAggregated.setInputNodesDetails(InputNodesDetailsAggregated);
			
			//setting the channel on which the detected events will be sent to GDI
			eventDetectionNodeAggregated.setChannel(transmisingChannel);

			// and deployed
			eventDetectionNodeAggregated.deployEventDetectionLogic(epService);
			
			// node is then added to the list of already deployed nodes
			listOfNodeIDsAggregated.add(eventDetectionNodeAggregated.getEventDetectionLogicID());

			//adding a new eventStream to GDI
			addNewEventStreamToGDIAggregated(eventDetectionNodeAggregated);
			
		}

	}

	/**
	 * Method that connects the stream with the inputStreamUUID
	 * 
	 * @param inputStreamUUID is the UUID of the stream we want to connect 
	 * @param exchange the name of the exchange to connect to
	 */
	public void connectNewStreamToTheEventDetection(String inputStreamUUID, String exchange) {

		StreamDetails streamDetails = new StreamDetails(inputStreamUUID);
		streamDetails.computeStreamDetails();
		
		if (streamDetails.isValid()) {
			
			if(exchange.equals(Commons.ANNOTATED_DATA_EXCHANGE))
			{
				//System.out.println("BB");
				DataBusConnector dataBusConnectorAnnotated = new DataBusConnector(dataBusChannel, streamDetails, epService, exchange);
				dataBusConnectorAnnotated.connectInputAdapter();
				
				// holds the bond between UUID and dataBusConnector objects 
				dataBusConnectorLinker.put(inputStreamUUID, dataBusConnectorAnnotated);
				
				// holds the link between UUIS and the name of the stream in Esper
				UUIDAndEsperNameLinker.put(inputStreamUUID, streamDetails.getStreamDescription()
						.getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange);
				
			}
			else if(exchange.equals(Commons.AGGREGATED_DATA_EXCHANGE))
			{
				DataBusConnector dataBusConnectorAggregated= new DataBusConnector(dataBusChannel, streamDetails, epService, exchange);
				dataBusConnectorAggregated.connectInputAdapter();
				
				//holds the bond between UUID and dataBusConnector objects 
				dataBusConnectorLinkerAggregated.put(inputStreamUUID, dataBusConnectorAggregated);
				
				// holds the link between UUIS and the name of the stream in Esper
				UUIDAndEsperNameLinkerAggregated.put(inputStreamUUID, streamDetails.getStreamDescription()
						.getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange);
				
			}
			
			logger.info("Succesfully connected new stream to ED, with UUID: "+inputStreamUUID);
		}
		
	}

	/**
	 * Method used to remove a stream from the bus.
	 * 
	 * @param inputStreamUUID is the UUID of the stream we want to remove/disconnect 
	 */
	private void disconnectNewStreamToTheEventDetection(String inputStreamUUID) {

		DataBusConnector dataBusConnector = dataBusConnectorLinker.get(inputStreamUUID);
		dataBusConnector.disconnectInputAdapter();
		
		dataBusConnector = dataBusConnectorLinkerAggregated.get(inputStreamUUID);
		dataBusConnector.disconnectInputAdapter();
		
		logger.info("Removed stream from ED, with UUID: "+inputStreamUUID);

	}
	
	/**
	 * Method used to register/add a new eventStream to GDI
	 * 
	 * @param eventDetectionNode is the event detection node that will be added
	 */
	private void addNewEventStreamToGDI(EventDetectionNode eventDetectionNode) {

		//generating a random unique UUID
		UUID x = UUID.randomUUID();

		try {
			// registering the event stream
			/*System.out.println("UUID = "+x);
			System.out.println("getOutputRoutingKey = "+eventDetectionNode.getOutputRoutingKey());
			System.out.println("getLongitude = "+eventDetectionNode.getEventCoordinate().getLongitude());
			System.out.println("getLatitude = "+eventDetectionNode.getEventCoordinate().getLatitude());*/
			cp_GDI_Interface.registerEventStream(x, eventDetectionNode.getOutputRoutingKey(),
					eventDetectionNode.getEventCoordinate().getLongitude(),
					eventDetectionNode.getEventCoordinate().getLatitude(), CpGdiInterface.EPSG_WGS84);

		} catch (SQLException e) {
			logger.error("Error registering new EventStream in GDI with UUID: " + eventDetectionNode.getOutputRoutingKey(), e);
		}

	}
	
	/**
	 * Method used to register/add a new eventStream to GDI. FOR AGGREGATED
	 * 
	 * @param eventDetectionNodeAggregated is the event detection node that will be added
	 */
	private void addNewEventStreamToGDIAggregated(EventDetectionNodeAggregated eventDetectionNodeAggregated) {

		//generating a random unique UUID
		UUID x = UUID.randomUUID();

		try {
			/*System.out.println("UUID = "+x);
			System.out.println("getOutputRoutingKey = "+eventDetectionNodeAggregated.getOutputRoutingKey());
			System.out.println("getLongitude = "+eventDetectionNodeAggregated.getEventCoordinate().getLongitude());
			System.out.println("getLatitude = "+eventDetectionNodeAggregated.getEventCoordinate().getLatitude());
			System.out.println("CpGdiInterface.EPSG_WGS84 = "+CpGdiInterface.EPSG_WGS84);*/
			
			// registering the event stream
			cp_GDI_Interface.registerEventStream(x, eventDetectionNodeAggregated.getOutputRoutingKey(),
					eventDetectionNodeAggregated.getEventCoordinate().getLongitude(),
					eventDetectionNodeAggregated.getEventCoordinate().getLatitude(), CpGdiInterface.EPSG_WGS84);

		} catch (SQLException e) {
			logger.error("Error registering new EventStream in GDI with UUID: " + eventDetectionNodeAggregated.getOutputRoutingKey(), e);
		}

	}
	
	/**
	 * Method that removes a node by its eventDetectionNodeID
	 * The list of already deployed nodes is updated with this change.
	 * 
	 * @param eventDetectionNodeID the ID of the node we want to remove
	 */
	public void removeEventDetectionNode(String eventDetectionNodeID) {

		// hashmap that contains the name of the stream and UUID
		HashMap<String, String> eventDetectionLogicInputMapingAggregated = deployedEventDetectionNodeConnectionsAggregated
				.get(eventDetectionNodeID);
		
		// hashmap that contains the name of the stream and UUID
		HashMap<String, String> eventDetectionLogicInputMaping = deployedEventDetectionNodeConnections
				.get(eventDetectionNodeID);

		ArrayList<String> list;

		listOfNodeIDs.remove(eventDetectionNodeID);
		listOfNodeIDsAggregated.remove(eventDetectionNodeID);

		for (String inputStreamUUID : eventDetectionLogicInputMaping.values()) {

			if (streamBindingToEventDetectionLogicModules.containsKey(inputStreamUUID)) {

				// list of all nodes connected to that particular stream
				list = streamBindingToEventDetectionLogicModules.get(inputStreamUUID);
				list.remove(eventDetectionNodeID);
				// if the list is empty after the node is removed, that means the stream is no longer needed, so we disconnect it.
				if (list.isEmpty()) {
					streamBindingToEventDetectionLogicModules.remove(inputStreamUUID);
					disconnectNewStreamToTheEventDetection(inputStreamUUID);
				}
			}
		
		}
		
		for(String inputStreamUUID : eventDetectionLogicInputMapingAggregated.values()) {
			
			if (streamBindingToEventDetectionLogicModulesAggregated.containsKey(inputStreamUUID)) {

				//list of all nodes connected to that particular stream
				list = streamBindingToEventDetectionLogicModulesAggregated.get(inputStreamUUID);
				list.remove(eventDetectionNodeID);
				//if the list is empty after the node is removed, that means the stream is no longer needed, so we disconnect it.
				if (list.isEmpty()) {
					streamBindingToEventDetectionLogicModulesAggregated.remove(inputStreamUUID);
					disconnectNewStreamToTheEventDetection(inputStreamUUID);
				}
			}
			
		}
		
		logger.info("Removed the node: "+eventDetectionNodeID);
	}

}
