# Event Detection Component (ED)

The event detection component is generic and can be used to deploy/execute event detection logic. Within the CityPulse project life time a suit of event detection mechanism have been developed (e.g. traffic jam detection, parking garages status changed detection). The application developer can, at any time, trigger the deployment of these mechanisms in order to analyse the data coming from the considered city. In addition to that these, the application developer, by implementing a java interface, can develop custom made event detection logic.

## Component location on GitHub

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Other CityPulse framework dependencies 

* The Event Detection (ED) component requires a connection to Geo-spatial data base (GDI) in order to send it events that it has detected in its deployed nodes.
* ED component also connects to the Data bus from where it gets the annotated and aggregated data.
* Resource Manager Component is another dependency for the ED. ED connects to it to get the sensors description, information that is later used in the interpretation on aggregated and annotated data.

### Component deployment steps and Configuration file

The Event Detection component is packed in a .jar file. To be used it has to be included in your project.
The component has a configuration file named config.properties that holds the configuration parameters required for connecting to several other components from the CityPulse framework such as:

* Resource Manager Configuration parameters: IP and Port.
* The Data bus Configuration parameters: IP and Port.
* Geo-spatial data base Configuration parameters: IP and Port.

The configuration file can be found in /resources within the .jar file and can be modified easily. 

## API method description, parameters and response

In the main method, the first thing to do is create e new EventDetection object. By doing this you will initialize Esper, GDI, connect to the message bus from where you get the aggregates/annotated messages and to the message bus used to send the newly detected events to GDI.

Then we need to create a new class that extends EventDetectionNode (let’s say it’s named ParkingEventDetectionNode) and initialize it in the main method you just created earlier, with the required parameters (plus the thresholds). Then, again in the main method, create a Coordinate object where you set the coordinates of the sensor. Then use the method setEventCoordinate on the ParkingEventDetectionNode object. You will also create a HashMap containing as key the UUID of the stream and as value the stream’s name (eg: parkingGarageData) the same name you used when creating your custom node (in the method getListOfInputStreamNames() ).

Then simply add the new custom node object to EventDetection using the method addEventDetectionNode().  
And that’s about it. In your custom node class ParkingEventDetectionNode in the method getEventDetectionLogic(EPServiceProvider epService) write all your Esper queries. When you get your desired event, simply create a ContextualEvent object with the required parameters and use the method sendEvent to send your ContextualEvent to GDI.


## Methods

Can be seen in the [Javadoc](/javadoc) file. 


## Authors

* **Serbanescu Bogdan Victor** - (https://github.com/sherbibv)



