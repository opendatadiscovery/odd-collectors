# Open Data Discovery GCP Collector local demo environment
* * *

The following is a set of instructions to run ODD GCP Collector locally using docker and docker-compose. 

This environment consists of:
* ODD Platform – an application that ingests, structurizes, indexes and provides a collected metadata via REST API and UI
* ODD Collector GCP – a lightweight service which gathers metadata from GCP

## Prerequisites

* Docker Engine 19.03.0+
* Preferably the latest docker-compose

## Step 1: Configuring and running ODD Platform

### Assumptions

* Port 8080 is free. Commands to check that might be:
    * Linux/Mac: `lsof -i -P -n | grep LISTEN | grep 8080`
    * Windows Powershell: `Get-NetTCPConnection | where Localport -eq 8080 | select Localport,OwningProcess`

### Execution

Run **from the project root folder** `docker-compose -f docker/demo.yaml up -d odd-platform`.

### Result

Open http://localhost:8080/ in your browser. You should be able to see an an empty catalog

## Step 2: Configuring and running GCP Collector to gather metadata

### Create Collector entity

1. Go to the http://localhost:8080/management/collectors and select `Add collector`
2. Complete the following fields:
    * **Name**
    * **Namespace** (optional)
    * **Description** (optional)
3. Click **Save**. Your collector should appear in the list
4. Copy the token by clicking **Copy** right to the token value

### Configure and run the Collector

1. Paste the token obtained in the previous step into the `docker/config/collector_config.yaml` file under the `token` entry. Replace `<YOUR_PROJECT>` with the ID of your GCP project.
2. Create a GCP API key using [this](https://cloud.google.com/docs/authentication/getting-started) documentation. Save this key to a file `docker/config/key.json`
3. Run **from the project root folder** `docker-compose -f docker/demo.yaml up -d odd-collector-gcp`.

### Result

1. Open http://localhost:8080/management/datasources in your browser

You should be able to see a new data source with the name `bigquery-storage`

2. Go to the **Catalog** section. Select the created data source in the `Datasources` filter

You should be able to see entities that GCP collector was able to gather from the GCP

### Troubleshooting

**My entities from the sample data aren't shown in the platform.**

Check the logs by running **from the project root folder** `docker-compose -f docker/demo.yaml logs -f`
**
