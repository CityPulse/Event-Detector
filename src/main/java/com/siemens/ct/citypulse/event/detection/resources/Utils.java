package com.siemens.ct.citypulse.event.detection.resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.siemens.ct.citypulse.event.detection.core.Commons;

public class Utils {

	private static Logger logger = Logger.getLogger(Utils.class);

	/**
	 * Method used to load the configuration parameters 
	 * 
	 * @param propertyToConfigure is the configuration property to load
	 * @return the value of the property you want to load
	 */
	public static String loadConfigurationProp(String propertyToConfigure) {

		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = Utils.class.getResourceAsStream(Commons.CONFIG_PROPERTIES_FILE_PATH);

			// load a properties file
			prop.load(input);

			// get the property value
			propertyToConfigure = prop.getProperty(propertyToConfigure);

		} catch (IOException e) {
			logger.error("Could not load configuration file ", e);

		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		logger.info("Succesfully loaded configuration property: "+propertyToConfigure);
		
		return propertyToConfigure;
	}
}
