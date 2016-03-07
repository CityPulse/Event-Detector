package com.siemens.ct.citypulse.event.detection.resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.siemens.ct.citypulse.event.detection.core.Commons;
import com.siemens.ct.citypulse.event.detection.resources.streamDescription.Field;
import com.siemens.ct.citypulse.event.detection.resources.streamDescription.StreamDescription;

public class StreamDetails {

	private static Logger logger = Logger.getLogger(StreamDetails.class);

	public static String resourceManagerConnectorIP = Utils.loadConfigurationProp("resourceManagerConnectorIP");

	public static String resourceManagerConnectorPort = Utils.loadConfigurationProp("resourceManagerConnectorPort");

	/**
	 * HashMap that links the field name and their data type
	 */
	private HashMap<String, Object> mapDef;

	/**
	 * List of fields extracted from the SPARQL Endpoint
	 */
	private ArrayList<String> fieldsFromRDF;
	
	private boolean valid = true;

	private String streamID;
	//HashMap that contains the link between  the field name and a string formed by concatenating the field name and UUID of stream (FIELD_NAME - UUID) 
	private HashMap<String, String> dataFieldsNames = new HashMap<String, String>();
	
	private StreamDescription streamDescription;

	public StreamDetails(String streamID) {
		super();
		this.streamID = streamID;
	}
	
	/**
	 * Method that gets the stream description object from JSON AND from Virtuoso
	 * We combine both methods of getting the stream description to create the mapDef HashMap that contains the fields and their type
	 * 
	 */
	public void computeStreamDetails() {

		// getting the streamDescription from JSON
		StreamDescription streamDescription = null;
		streamDescription = getStreamdescriptionFromJSON(streamID);
		
		//System.out.println("streamDescription : "+streamDescription.getData().getSensorType());
	
		// getting the fields from the RDF description on Virtuoso Store
		fieldsFromRDF = getStreamDescriptionFromRDF(streamID);
		
		//System.out.println("Fields from RDF : "+fieldsFromRDF);
		
		// obtaining the fields and their type
		mapDef = getInputField(streamDescription.getData().getField_array(), fieldsFromRDF);
	
	}

	/**
	 * Method used to get the description(fields) of a particular sensor via Virtuoso
	 * 
	 * @param streamID the UUID of the stream
	 * @return an arrayList containing the names of all the fields of that particular sensor with UUID = streamID
	 */
	private ArrayList<String> getStreamDescriptionFromRDF(String streamID) {

		fieldsFromRDF = new ArrayList<String>();
		//System.out.println("streamID : "+streamID);
		
		// creating a query to send to Virtuoso sparql endpoint		
		String query_SPARQL = "PREFIX DUL: <http://www.loa-cnr.it/ontologies/DUL.owl#> "
				+ "PREFIX ces: <http://www.insight-centre.org/ces#> " 
				+ "PREFIX ct: <http://ict-citypulse.eu/city#> "
				+ "PREFIX daml: <http://www.daml.org/2001/03/daml+oil#> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> " 
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> "
				+ "PREFIX muo: <http://purl.oclc.org/NET/muo/muo#> " 
				+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
				+ "PREFIX owls: <http://www.daml.org/services/owl-s/1.2/Service.owl#> "
				+ "PREFIX owlsg: <http://www.daml.org/services/owl-s/1.2/Grounding.owl#> "
				+ "PREFIX owlss: <http://www.daml.org/services/owl-s/1.2/Service.owl#> "
				+ "PREFIX owlssc: <http://www.daml.org/services/owl-s/1.2/ServiceCategory.owl#> "
				+ "PREFIX owlssp: <http://www.daml.org/services/owl-s/1.2/Profile.owl#> "
				+ "PREFIX owlssrp: <http://www.daml.org/services/owl-s/1.2/ServiceParameter.owl#> "
				+ "PREFIX prov: <http://www.w3.org/ns/prov#> " 
				+ "PREFIX qoi: <http://purl.oclc.org/NET/UASO/qoi#> "
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX sao: <http://purl.oclc.org/NET/UNIS/sao/sao#> "
				+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
				+ "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> " 
				+ "PREFIX time: <http://www.w3.org/2006/time#> "
				+ "PREFIX tl: <http://purl.org/NET/c4dm/timeline.owl#> "
				+ "PREFIX tzont: <http://www.w3.org/2006/timezone#> "
				+ "PREFIX vs: <http://www.w3.org/2003/06/sw-vocab-status/ns#> "
				+ "PREFIX wot: <http://xmlns.com/wot/0.1/> " 
				+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "PREFIX xml: <http://www.w3.org/XML/1998/namespace> "
				+ "PREFIX ns1: <http://www.daml.org/services/owl-s/1.2/ServiceCategory.owl#> " 
				+ "select ?x "
				+ " where {" 
				+ "<http://ict-citypulse.eu/PrimitiveEventService-" + streamID+ "> owls:supports ?grounding.  " 
				+ "?grounding ces:hasExchangeName ?exchange. "
				+ "?grounding ces:hasTopic ?topic.   " 
				+ "<http://ict-citypulse.eu/PrimitiveEventService-" + streamID+ ">  ces:hasPhysicalEventSource ?sensor. " 
				+ "?sensor ssn:observes ?x. " 
				+ "?x rdf:type ?pType. "
				+ "Filter (str(?exchange)=\"annotated_data\")" 
				+ "}";

		// sparql request
		String SPARQL_endpoint = "http://131.227.92.55:8011/sparql/";

		QueryExecution x = QueryExecutionFactory.sparqlService(SPARQL_endpoint, query_SPARQL);
		ResultSet results = x.execSelect();
		
		while (results.hasNext()) {
			QuerySolution thisRow = results.next();
			Resource myResource = thisRow.getResource("x");
			String myString = myResource.toString();
			
			myString = myString.substring(myString.lastIndexOf("#") + 1, myString.length());
			fieldsFromRDF.add(myString);
		}
		
		if(fieldsFromRDF.isEmpty())
		{
			valid = false;
		}
		
		return fieldsFromRDF;

	}
	/**
	 * Method used to link the fields with their data type using both the Virtuoso stream description and JSON stream description
	 * 
	 * @param fields A list of all the fields 
	 * @param fieldsFromRDF list of all fields found with the method getVirtuosoSensorDescription.
	 * @return a hashMap containing the link between the field name and it's type
	 */
	private HashMap<String, Object> getInputField(List<Field> fields, ArrayList<String> fieldsFromRDF) {

		HashMap<String, Object> mapDef = new HashMap<String, Object>();

		for (Field field : fields) {

			//getting the field name from the propertyURI field of the StreamDescription object
			String parsedPropertyName = field.getPropertyURI().substring(field.getPropertyURI().indexOf("#")+1, field.getPropertyURI().length());
			//System.out.println(parsedPropertyName);
			
			if (field.getDataType().equals("enum")) {
				mapDef.put(parsedPropertyName, Enum.class);
			} else if (field.getDataType().equals("str")) {
				mapDef.put(parsedPropertyName, String.class);
			} else if (field.getDataType().equals("datetime.datetime")) {
				mapDef.put(parsedPropertyName, String.class);
			} else if (field.getDataType().equals("int")) {
				mapDef.put(parsedPropertyName, Integer.class);
			} else if (field.getDataType().equals("double")) {
				mapDef.put(parsedPropertyName, Double.class);
			} else if (field.getDataType().equals("float")) {
				mapDef.put(parsedPropertyName, Float.class);
			} else {
				mapDef.put(parsedPropertyName, field.getDataType().getClass());
			}				
		}
		
		for (String key : mapDef.keySet()) {
			for (String fieldFromRDF : fieldsFromRDF) {
				//we take the substring because the annotatedField variable contains the "name_of_the_field-UUID"
				if (fieldFromRDF.substring(0,fieldFromRDF.indexOf("-")).toUpperCase().equals(key.toUpperCase())) {
					//we form the HashMap with the full "name_of_the_field-UUID"
					dataFieldsNames.put(key, fieldFromRDF);
				}
			}
		}
		
		if(dataFieldsNames.isEmpty())
		{
			valid = false;
		}

		return mapDef;

	}
	/**
	 * Method used to get the stream's description via JSON format
	 * 
	 * @param streamID the stream UUID 
	 * @return an StreamDescription object that is mapped on the JSON recieved 
	 */
	private StreamDescription getStreamdescriptionFromJSON(String streamID) {

		String URLString = Commons.GET_DESCRIPTION_URL;
		URLString = URLString.replace("#", resourceManagerConnectorIP + ":" + resourceManagerConnectorPort);
		URLString = URLString + streamID;

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

		try {
			streamDescription = getStreamDescriptionObject(message.toString());
		} catch (JsonSyntaxException e) {
			logger.error("Cannot convert JSON: " + message.toString() + " to object: ", e);
			valid = false;
			return null;
		}

		return streamDescription;
	}

	/**
	 * Method used to map the JSON to a StreamDescription object
	 * 
	 * @param responseFromUUIDString
	 * @return returns a StreamDescriotion object
	 */
	private StreamDescription getStreamDescriptionObject(String responseFromUUIDString) {

		Gson gson = new Gson();
		StreamDescription streamDescription = gson.fromJson(responseFromUUIDString, StreamDescription.class);
		
		return streamDescription;
	}

	public HashMap<String, String> getDataFieldsNames() {
		return dataFieldsNames;
	}

	public StreamDescription getStreamDescription() {
		return streamDescription;
	}

	public boolean isValid() {
		return valid;
	}

	public HashMap<String, Object> getMapDef() {
		return mapDef;
	}

}
