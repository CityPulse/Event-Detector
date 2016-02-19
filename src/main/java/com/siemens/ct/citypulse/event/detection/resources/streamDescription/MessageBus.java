package com.siemens.ct.citypulse.event.detection.resources.streamDescription;


public class MessageBus {
	
	private String routingKey;

	public MessageBus() {
		super();
	}
	
	public MessageBus(String routingKey) {
		super();
		this.routingKey = routingKey;
	}

	public String getRoutingKey() {
		return routingKey;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}
	
	
}
