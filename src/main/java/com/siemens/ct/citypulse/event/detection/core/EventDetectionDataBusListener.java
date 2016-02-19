package com.siemens.ct.citypulse.event.detection.core;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPServiceProvider;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

class EventDetectionDataBusListener implements Consumer {

	private Logger logger = Logger
			.getLogger(EventDetectionDataBusListener.class);

//	private EPServiceProvider epService;
	private String CEPTopic;

	public EventDetectionDataBusListener(EPServiceProvider epService,
			String CEPTopic) {
		super();
		//this.epService = epService;
		this.CEPTopic = CEPTopic;
	}

	public void handleCancel(String arg0) throws IOException {

	}

	public void handleCancelOk(String arg0) {

	}

	public void handleConsumeOk(String arg0) {

	}

	public void handleDelivery(String arg0, Envelope arg1,
			BasicProperties arg2, byte[] arg3) throws IOException {

		String msgBody = new String(arg3);
		logger.info("EventDetection: Message received. topic: " + this.CEPTopic
				+ " " + "message: " + msgBody);

		// TODO: to be delivered to the CEP engine

	}

	public void handleRecoverOk(String arg0) {

	}

	public void handleShutdownSignal(String arg0, ShutdownSignalException arg1) {

	}

}
