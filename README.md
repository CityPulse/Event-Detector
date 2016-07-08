# Event Detection Component (ED)

The Event detection component provides the generic tools for processing the annotated as well as aggregated data streams to obtain events occurring into the city. This component is highly ﬂexible in deploying new event detection mechanisms, since diﬀerent smart city applications require diﬀerent events to be detected from the same data sources.

Figure 1 depicts the architecture of the proposed framework which allows event detection for sensory data streams. It provides a set of generic tools which can be used for detecting an event. In order to detect a certain event, the domain expert, has to deploy a piece of code called event detection node. These nodes contain the event detection logic, which represent the pattern that has to be identiﬁed during raw data processing.

![alt tag](https://github.com/CityPulse/EventDetection/blob/master/ED_arhitecture.png "Figure 1 Event detection component main blocks and the dependent components.") 


A domain expert, with some help from an application developer, can use two conﬁguration interfaces displayed by the framework in order to perform the following activities:
*	Describe how to access the streams endpoints and how to interpret the received messages on a design time phase;
*	Register the data streams from the city by providing their descriptions;
*	Develop the event detection nodes on a design time phase;
*	Deploy the event detection nodes for various stream combinations.
	
Within the CityPulse project life time a suit of event detection nodes have been developed (e.g. traffic jam detection, parking garages status changed detection). The application developer can, at any time, trigger the deployment of these mechanisms in order to analyse the data coming from the considered city. In addition to that these, the application developer, by implementing a java interface, can develop custom made event detection logic.
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.


### Other CityPulse framework dependencies 

*	The Event Detection (ED) component requires a connection to Geo-spatial data base (GDI) in order to send it events that it has detected in its deployed nodes.
*	ED component also connects to the Data bus from where it gets the annotated and aggregated data.
*	Resource Manager Component is another dependency for the ED. ED connects to it to get the sensors description, information that is later used in the interpretation on aggregated and annotated data.


### Component deployment steps and Configuration file

The Event Detection component is packed in a .jar file. To be used it has to be included in your project. The component has a configuration file named config.properties that holds the configuration parameters required for connecting to several other components from the CityPulse framework such as:

* Resource Manager Configuration parameters: IP and Port.
* The Data bus Configuration parameters: IP and Port.
* Geo-spatial data base Configuration parameters: IP and Port.
* Testing mode parameter: testParameter (Boolean type).
* The URI for GDI parameter: GDI_AMQP_URI.

The configuration file can be found in /resources within the .jar file and can be modified easily. 

## API method description, parameters and response

In the main method, the first thing to do is create e new *EventDetection* object. By doing this you will initialize Esper, GDI, connect to the message bus from where you get the aggregates/annotated messages and to the message bus used to send the newly detected events to GDI.

Then we need to create a new class that extends *EventDetectionNode* (let us say it is named *ParkingEventDetectionNode*) and initialize it in the main method you just created earlier, with the required parameters (plus the thresholds). Then, again in the main method, create a Coordinate object where you set the coordinates of the sensor. Then use the method *setEventCoordinate* on the *ParkingEventDetectionNode* object. You will also create a HashMap containing as key the UUID of the stream and as value the stream’s name (eg: *parkingGarageData*) the same name you used when creating your custom node (in the method *getListOfInputStreamNames()* ).

Then simply add the new custom node object to *EventDetection* using the method *addEventDetectionNode()*.
And that’s about it. In your custom node class *ParkingEventDetectionNode* in the method *getEventDetectionLogic(EPServiceProvider epService)* write all your Esper queries. When you get your desired event, simply create a *ContextualEvent* object with the required parameters and use the method *sendEvent* to send your *ContextualEvent* to GDI.


## Methods

The java project and the documentations can be found on Github: https://github.com/CityPulse/EventDetection . 


## Contributers

The Event Detection component was developed as part of the EU project CityPulse. The consortium member Siemens provided the main contributions for this component.


## Authors

* **Serbanescu Bogdan Victor** - (https://github.com/sherbibv)



