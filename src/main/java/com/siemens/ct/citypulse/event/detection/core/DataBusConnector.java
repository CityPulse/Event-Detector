package com.siemens.ct.citypulse.event.detection.core;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPServiceProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.siemens.ct.citypulse.event.detection.resources.ObservationUtils;
import com.siemens.ct.citypulse.event.detection.resources.StreamDetails;

public class DataBusConnector extends DefaultConsumer {

	public Logger logger = Logger.getLogger(DataBusConnector.class);

	private Channel dataBusChannel;

	private StreamDetails streamDetails;

	private EPServiceProvider epService;

	private HashMap<String, Object> aggregatedMapValues;
	
	private HashMap<String, Object> annotatedMapValues;

	private String channelTag;
	
	private String exchange;

	public DataBusConnector(Channel dataBusChannelRecieved, StreamDetails streamDetails,
			EPServiceProvider epServiceRecieved, String exchange) {

		super(dataBusChannelRecieved);
		this.dataBusChannel = dataBusChannelRecieved;
		this.streamDetails = streamDetails;
		this.epService = epServiceRecieved;
		this.exchange = exchange;

	}

	/**
	 * Method used to add a new stream to Esper. First param is the name of the Stream(routing key of the stream with the . replaced with a _ )
	 * and the second is the definition of the fields(HashMap with key - name of field and value - data type) 
	 * Also we connect to a message bus to listen for messages coming for that specific sensor.
	 * 
	 */
	public void connectInputAdapter() {
		
		epService.getEPAdministrator().getConfiguration().addEventType(
				streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange,
				streamDetails.getMapDef());

		
		logger.info("Registerd event type to esper:"
				+ streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey());
				
		try {
			String queueName = dataBusChannel.queueDeclare().getQueue();
			dataBusChannel.queueBind(queueName, exchange, streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey());
			channelTag = dataBusChannel.basicConsume(queueName, false, this);
		} catch (IOException e) {
			logger.error("DataBusChannel could not connect to queue: " + e);
		}
		
	}
	
	/**
	 * Auto generated method that handles the incoming messages from the data bus for a specific stream 
	 * The message is in RDF format, that's why we need to deserialize the RDF with the method deserializeRDF
	 * that returns a HashMap that links the name of the field of the stream and it's data type
	 * 
	 */
	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
		
		String dataMessage = null;
		
		try {
			dataMessage = new String(body, "UTF-8");
			/*System.out.println("===");
			System.out.println(dataMessage);
			System.out.println("===");*/
		} catch (UnsupportedEncodingException e) {
			logger.error("Error due to unsupported encoding while parsing observation: ", e);
		}
		
		if(exchange.equals(Commons.ANNOTATED_DATA_EXCHANGE))
		{
			//getting the values from annotated data
			annotatedMapValues = ObservationUtils.deserializeRDFfromAnnotatedMessage(dataMessage, streamDetails.getDataFieldsNames(),
					streamDetails.getMapDef());
			//System.out.println(annotatedMapValues+ " for "+streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey() );
			
			//if all the fields have a value != null, then we can apply the CEP logic
			if(!annotatedMapValues.containsValue(null))
			{
				//System.out.println("annotatedMapValues routingKey: "+streamDetails.getStreamDescription().getData().getSensorID()+" : "+annotatedMapValues);
				epService.getEPRuntime().sendEvent(annotatedMapValues,
						streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange);
			}
			try {
				dataBusChannel.basicAck(envelope.getDeliveryTag(), false);
			} catch (IOException e) {
				logger.error("Error while trying to acknoledge the message recieved from the data bus", e);
			}
		}
		if(exchange.equals(Commons.AGGREGATED_DATA_EXCHANGE))
		{	
			//Updating data from aggregated data message
			aggregatedMapValues = ObservationUtils.deserializeRDFfromAggregatedMessage(dataMessage, streamDetails.getDataFieldsNames(),
					streamDetails.getMapDef(), aggregatedMapValues);
	
			//if all the fields have a value != null, then we can apply the CEP logic
			if(!aggregatedMapValues.containsValue(null))
			{
				//System.out.println("All fields of aggregated map != null");
				epService.getEPRuntime().sendEvent(aggregatedMapValues,
						streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange);
			}			
			try {
				dataBusChannel.basicAck(envelope.getDeliveryTag(), false);
			} catch (IOException e) {
				logger.error("Error while trying to acknoledge the message recieved from the data bus", e);
			}
		}

	}

	/**
	 * Method used to disconnect a stream from the message bus and also Esper
	 * 
	 */
	public void disconnectInputAdapter() {

		epService.getEPAdministrator().getConfiguration().removeEventType(
				streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey().replace(".", "_")+"_"+exchange, true);

		logger.info("Removed event type from esper:"
				+ streamDetails.getStreamDescription().getData().getMessagebus().getRoutingKey());

		try {
			dataBusChannel.basicCancel(channelTag);
		} catch (IOException e) {
			logger.error("Error while cancelling the consumer with channelTag: "+channelTag, e);
		}

	}

}
