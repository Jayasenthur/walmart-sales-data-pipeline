## Walmart Sales Data Pipeline with PySpark

### Overview

This project demonstrates a data pipeline for analyzing Walmart sales data using Apache Spark and PySpark in Python. The pipeline loads data from Excel files, processes it with Spark SQL, and visualizes outputs with matplotlib. Data insights are saved as images and files for easy access.

### Prerequisites

Before you begin, ensure you have the following installed:

* **Java Development Kit (JDK):** Apache Spark requires Java to run. Install Java JDK 8 or higher.Download the latest version from Oracle's website.
* **Apache Spark:** Download and install the latest version from the Apache Spark website.
    * Set up Environment Variable
        * Create a `SPARK_HOME` environment variable pointing to your Spark installation directory.
        * Add $SPARK_HOME/bin to your system PATH for easy command-line access.
* **Python:** Install Python 3.x.
* **Required Python Libraries:** Install necessary libraries like `pandas`  and `matplotlib` using pip:

```bash
pip install pandas  matplotlib
```


### Setting up the Spark Environment
* **Start Spark in Command Line:**
  To initialize Spark in local mode, open your command prompt and type:
  ```bash
  pyspark
  ```
This will open an interactive Spark shell where you can directly execute Spark code.

* **Apache Spark Web UI:**
The Spark Web UI helps monitor tasks, stages, and resource allocation. By default, it is accessible at:
* Running locally (http://localhost:4040)
* Docker: [http://host.docker.internal:4040](http://host.docker.internal:4040)









