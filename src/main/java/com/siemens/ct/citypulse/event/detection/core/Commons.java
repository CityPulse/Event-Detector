package com.siemens.ct.citypulse.event.detection.core;

import com.siemens.ct.citypulse.event.detection.resources.Utils;

public class Commons {
	
	// configuration parameters
	public static String resourceManagerConnectorIP = Utils.loadConfigurationProp("resourceManagerConnectorIP");
	public static String resourceManagerConnectorPort = Utils.loadConfigurationProp("resourceManagerConnectorPort");
	public static String dataBusIP = Utils.loadConfigurationProp("dataBusIP");
	public static String dataBusPort = Utils.loadConfigurationProp("dataBusPort");
	public static String GDIhostname = Utils.loadConfigurationProp("GDIhostname");
	public static int GDIport = Integer.parseInt(Utils.loadConfigurationProp("GDIport"));
	public static boolean fakeInjectionMode = Boolean.parseBoolean(Utils.loadConfigurationProp("fakeInjectionMode"));
	public static String GDI_AMQP_URI = Utils.loadConfigurationProp("GDI_AMQP_URI");
	public static String SPARQL_ENDPOINT = Utils.loadConfigurationProp("SPARQL_ENDPOINT");

	public final static String RAW_DATA_EXCHANGE = "data";
	public final static String ANNOTATED_DATA_EXCHANGE = "annotated_data";
	public final static String AGGREGATED_DATA_EXCHANGE = "aggregated_data";
	public final static String CONFIG_PROPERTIES_FILE_PATH = "/config.properties";
	public final static String AARHUS_TRFFIC_UUIDs = "/UUID_and_coordinates_Traffic.txt";
	public final static String AARHUS_PARKING_UUIDs = "/UUID_and_coordinates_Parking.txt";
	public final static String GET_DESCRIPTION_URL = "http://#//api/get_description?uuid=";
	public final static String GET_RM_STATUS = "http://"+resourceManagerConnectorIP+":"+resourceManagerConnectorPort+"/api/get_status";
	public final static String EVENTS_EXCHANGE = "events";
	public final static String EVENTS_QUEUE = "q_events";
			
}