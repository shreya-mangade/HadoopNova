# HadoopNova (Data Transformation Pipeline with Hadoop)

This project implements a pipeline to transform large datasets between formats, such as CSV and JSON, using Hadoop’s core components, HDFS and MapReduce. The pipeline enables efficient data storage, transformation, and output by distributing data across nodes for fault tolerance and processing transformations in parallel. This solution is particularly useful for fields like healthcare and technology, where flexible data formats support integration, analysis, and API testing. HDFS provides scalability and reliability, making it ideal for handling unstructured data from diverse sources.

## Hadoop Cluster Commands

Here are some useful commands to manage the Hadoop cluster:

| Command                                      | Usage                                                 |
|----------------------------------------------|-------------------------------------------------------|
| `start-all.sh`                               | Starts all the daemons of the Hadoop cluster          |
| `stop-all.sh`                                | Stops all the daemons of the Hadoop cluster           |
| `./mr-jobhistory-daemon.sh start historyserver` | Starts the job application history server           |

Use these commands to start and stop your Hadoop cluster daemons and manage job history.

---

## Getting Started

To run the pipeline:

1. **Start the Hadoop Cluster**: Use the command `start-all.sh` to initialize the necessary daemons.
2. **Transform Data**: Run the MapReduce jobs to process and convert data between formats like CSV and JSON.
3. **Stop the Cluster**: Once processing is complete, use `stop-all.sh` to safely stop all daemons.

## Requirements

To run this project, ensure the following are installed and configured on your system:

- **Hadoop**: For distributed storage and data processing.
- **Maven**: For project build and dependency management.
- **Spring Boot**: To create java application.
- **JDK**: Java Development Kit required for compiling and running Java code.
- **Tomcat**: For deploying the Spring Boot application as a web server.
- **Thymeleaf**: A template engine integrated with Spring Boot for dynamic HTML generation.

## Example Use Case

The pipeline is particularly useful for fields with diverse data formats, such as:
- **Healthcare**: Integrates and analyzes unstructured patient data.
- **Technology**: Supports flexible data formats for API testing and data transformation.
