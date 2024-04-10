# Platform for short-term rental housing Stream Processing with Apache Flink
This code demonstrates a stream processing pipeline for an Airbnb-like booking platform using Apache Flink.

## Overview
The main functionality of this application includes:

### 1. Reading data from Kafka:
  The application reads booking event data from a Kafka topic, which acts as a message broker, allowing different systems to exchange data.

### 2. Event processing:
  - The EventParsingFunction extracts the relevant information from each booking event, such as the property ID, event type (e.g., create, cancel), and user ID. This information is captured in a 3-tuple data structure.
  - The EventAggregationFunction groups the processed events by property ID and event type, counting the occurrences of each combination. This gives us the number of times each event has occurred for each property.
### 3. Anomaly detection:
  - The AnomalyDetectionFunction checks if the event count for a particular property and event type is unusually high or low, which could indicate an anomaly. In this example, we consider event counts above 100 or below 10 as anomalies.
  - The AnomalyExtractionFunction extracts the anomalous events into a separate 2-tuple data structure containing the property ID, event type, and event count.
### 4. Data output:
  - The regular (non-anomalous) processed events are written to one Kafka topic.
  - The detected anomalous events are written to a separate Kafka topic.
    
## Usage
This code demonstrates how Apache Flink can be used to process real-time booking data for an Airbnb-like system. It performs aggregation, grouping, and anomaly detection on the data stream, which can provide valuable insights and help monitor the platform's activities.

## Implementation
The main components of the implementation are:

### 1. `EventParsingFunction`: 
Extracts relevant information from each booking event.
### 2. `EventAggregationFunction`: 
Groups the processed events by property ID and event type, counting the occurrences.
### 3. `AnomalyDetectionFunction`: 
Checks for unusually high or low event counts, indicating potential anomalies.
### 4. `AnomalyExtractionFunction`: 
Extracts the anomalous events into a separate data structure.
The processed events and detected anomalies are then written to separate Kafka topics for further analysis and monitoring.

## Conclusion
This code demonstrates the use of Apache Flink for real-time stream processing in an Airbnb-like platform. The application performs event processing, anomaly detection, and data output, providing a foundation for building more complex monitoring and analytics solutions.
