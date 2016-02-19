package com.siemens.ct.citypulse.event.detection.resources.streamDescription;

public class Field {

	private String propertyURI;
	private String propertyName;
	private String dataType;
	private String name;
	
	public Field() {
		super();
	}
	public Field(String propertyURI, String propertyName, String dataType, String name) {
		super();
		this.propertyURI = propertyURI;
		this.propertyName = propertyName;
		this.dataType = dataType;
		this.setName(name);
	}
	
	public String getPropertyURI() {
		return propertyURI;
	}
	public void setPropertyURI(String propertyURI) {
		this.propertyURI = propertyURI;
	}
	public String getPropertyName() {
		return propertyName;
	}
	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
}
