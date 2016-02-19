package com.siemens.ct.citypulse.event.detection.resources;

import java.io.ByteArrayInputStream;
import java.util.HashMap;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class ObservationUtils {

	/**
	 * Method used to deserialize a RDf message to obtain the values send on the message bus.
	 * 
	 * @param annotatedDataMessage is the RDf string we want to process
	 * @param dataFieldsNames is a HashMap linking the field name and the field name with the UUID concatenated( FIELD_NAME-UUID )
	 * @param mapDef a HashMap linking the field name and its data type
	 * @return a HashMap linking the field name and its read value
	 */
	public static HashMap<String, Object> deserializeRDFfromAnnotatedMessage(String annotatedDataMessage,
			HashMap<String, String> dataFieldsNames, HashMap<String, Object> mapDef) {
		
		//System.out.println(annotatedDataMessage);
		
		Model model = ModelFactory.createMemModelMaker().createFreshModel();
				
		model.read(new ByteArrayInputStream(annotatedDataMessage.getBytes()), null, "TTL");
		HashMap<String, Object> mapValues = new HashMap<String, Object>();
				
		for (String key : dataFieldsNames.keySet()) {

			//System.out.println(key + " # " + annotatedDataFieldsNames.get(key));
			
			//to hold the extracted value from the RDF
			String queryString = null;
			
			// creating a query
			queryString = "PREFIX DUL: <http://www.loa-cnr.it/ontologies/DUL.owl#> "
					+ "PREFIX ces: <http://www.insight-centre.org/ces#> "
					+ "PREFIX ct: <http://ict-citypulse.eu/city#> "
					+ "PREFIX daml: <http://www.daml.org/2001/03/daml+oil#> "
					+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> " 
					+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
					+ "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> "
					+ "PREFIX muo: <http://purl.oclc.org/NET/muo/muo#> "
					+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
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
					+ "PREFIX xml: <http://www.w3.org/XML/1998/namespace> "
					+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " 
					+ "SELECT ?value " 
					+ "WHERE {"
					+ " ?x a sao:Point . " 
					+ " ?x ssn:observedProperty ct:" + dataFieldsNames.get(key) + " . " 
					+ " ?x sao:value ?value . "
					+ "}";
						
			Query queryToProcess = QueryFactory.create(queryString);
			
			// Execute the query and obtain results
			QueryExecution queryExecutionToProcess = QueryExecutionFactory.create(queryToProcess, model);
			ResultSet resultsFromQuery = queryExecutionToProcess.execSelect();
			

			Object desiredValueLiteral = null;
			if (resultsFromQuery.hasNext()) {
				QuerySolution thisRow = resultsFromQuery.next();
				
				
				if(mapDef.containsKey(key))
				{
					if(mapDef.get(key) == Float.class)
					{
						desiredValueLiteral = ((Literal) thisRow.get("value")).getFloat();
					}
					else if(mapDef.get(key) == Integer.class)
					{
						String desiredValueLiteralString = ((Literal) thisRow.get("value")).getString();
						//System.out.println("desiredValueLiteralString : " + desiredValueLiteralString);
						if(desiredValueLiteralString.contains("."))
						{
							desiredValueLiteralString = desiredValueLiteralString.substring(0, desiredValueLiteralString.indexOf("."));
							//System.out.println("desiredValueLiteralString without . : " + desiredValueLiteralString);
						}
						desiredValueLiteral = Integer.parseInt(desiredValueLiteralString);
					}
					else if(mapDef.get(key) == Double.class)
					{
						desiredValueLiteral = ((Literal) thisRow.get("value")).getDouble();
					}
					else if(mapDef.get(key) == String.class)
					{
						desiredValueLiteral = ((Literal) thisRow.get("value")).getString();
					}
				}
				
				mapValues.put(key, desiredValueLiteral);
			}
			queryExecutionToProcess.close();
		}

		//System.out.println("MapValues annotated : " +mapValues);
		return mapValues;
	}
	
	public static HashMap<String, Object> deserializeRDFfromAggregatedMessage(String aggregatedDataMessage,
			HashMap<String, String> dataFieldsNames, HashMap<String, Object> mapDef, HashMap<String, Object> aggregatedMapValues) {
		
		//System.out.println("Aggregated Deserialisation");
		
		Model model = ModelFactory.createMemModelMaker().createFreshModel();
				
		model.read(new ByteArrayInputStream(aggregatedDataMessage.getBytes()), null, "TTL");
		HashMap<String, Object> mapValues = new HashMap<String, Object>();

		// to hold the extracted value from the RDF
		String queryString = null;

		// creating a query
		queryString = "PREFIX DUL: <http://www.loa-cnr.it/ontologies/DUL.owl#> "
				+ "PREFIX ces: <http://www.insight-centre.org/ces#> " 
				+ "PREFIX ct: <http://ict-citypulse.eu/city#> "
				+ "PREFIX daml: <http://www.daml.org/2001/03/daml+oil#> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> " 
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> "
				+ "PREFIX muo: <http://purl.oclc.org/NET/muo/muo#> " 
				+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
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
				+ "PREFIX xml: <http://www.w3.org/XML/1998/namespace> "
				+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " 
				+ "SELECT ?value ?field " 
				+ "WHERE {"
				+ " ?x a sao:SymbolicAggregateApproximation . " 
				+ " ?x prov:wasAttributedTo ?field . "
				+ " ?x sao:value ?value . " 
				+ "}";

		Query queryToProcess = QueryFactory.create(queryString);

		// Execute the query and obtain results
		QueryExecution queryExecutionToProcess = QueryExecutionFactory.create(queryToProcess, model);
		ResultSet resultsFromQuery = queryExecutionToProcess.execSelect();

		String field;
		String extractedValue;
		if (resultsFromQuery.hasNext()) {
			QuerySolution thisRow = resultsFromQuery.next();

			field = thisRow.get("field").toString();
			extractedValue = thisRow.get("value").toString();

			//System.out.println("Field : " + field);
			//System.out.println("Value : " + extractedValue);

			// some fields appear without a prefix => a parsing has to be 
			// done, to extract the name of the field witch is after the #
			// character
			if (field.contains("#"))
				field = field.substring(field.indexOf("#") + 1, field.length());

			for (String key : mapDef.keySet()) {
				if (key.equals(field)) {
					// converting from a char to it's equivalent in ASCII
					char extractedValueChar = extractedValue.charAt(0);
					int extractedValueInteger = (int)extractedValueChar;
					System.out.println("Extracted char : "+extractedValueChar);
					mapValues.put(key, extractedValueInteger-97);
				} else {
					mapValues.put(key, null);
				}
			}

			queryExecutionToProcess.close();
		}
		
		
		//System.out.println("mapValues before : "+mapValues);
		
		
		//the key = the field name
		for (String key : mapValues.keySet()) 
		{	
			// the fields that were not updated within the aggregatedMessage
			// received will be updated with its last values stored in
			// aggregatedMapValues
			if (mapValues.get(key) == null) 
			{
				if (aggregatedMapValues == null) 
				{
					aggregatedMapValues = mapValues;
				} 
				else 
				{
					if (aggregatedMapValues.containsKey(key)) 
					{
						// if the field that was not updated in the
						// aggregatedMessage has a previous value stored in
						// aggregatedMapValues
						if (aggregatedMapValues.get(key) != null) 
						{
							// then we update the mapValues with that value
							mapValues.put(key, aggregatedMapValues.get(key));
						}
					}
				}
			}
		}
		
		//System.out.println("aggregatedMapValues : "+aggregatedMapValues);
		//System.out.println("mapValues after : "+mapValues);
		
		return mapValues;
	}

}
