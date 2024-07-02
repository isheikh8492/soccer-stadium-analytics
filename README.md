# Soccer Stadium Analytics ETL Project

## Project Overview

This project uses Apache Airflow, Docker, and various APIs to build an ETL pipeline for analyzing soccer stadium data. The pipeline extracts data from Wikipedia, processes it, and stores it in Azure Storage, to be visualizable on Tableau.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Docker Desktop installed on your machine ([Download Docker Desktop](https://www.docker.com/products/docker-desktop))
- Python 3.8+ installed on your machine

## Setup Instructions

### 1. Clone the Repository

Clone this project to your local machine using:

```bash
git clone <repository-url>
cd soccer-stadium-analytics
```

### 2. Obtain API Keys

You need the following API keys:
- **Google Maps API Key**: [Get your API key](https://developers.google.com/maps/documentation/javascript/get-api-key)
- **Azure Storage Account Key**: [Get your Storage Account Key](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage)
- **Airflow Webserver Secret Key**: This key is used to enhance security for your Airflow instance. You can generate a random string for this purpose.

### 3. Create a `.env` File

Create a `.env` file in the root directory of the project and add your keys:

```env
AIRFLOW_WEBSERVER_SECRET_KEY=your_airflow_secret_key
AZURE_STORAGE_ACCOUNT_KEY=your_azure_storage_account_key
GOOGLE_MAPS_GEOCODER_KEY=your_google_maps_api_key
```

### 4. Create a Virtual Environment and Install Dependencies

Create a virtual environment and install the required Python packages:

```bash
python -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows

pip install -r requirements.txt
```

### 5. Run Docker Containers

Start the Docker containers:

```bash
docker-compose up -d
```

To stop and remove all containers and volumes:

```bash
docker-compose down --volumes
```

### 6. Access Airflow

Ensure all Docker containers are running:

```bash
docker ps
```

Open your web browser and go to [http://localhost:8080](http://localhost:8080) to access the Airflow web interface.

- **Username**: `admin`
- **Password**: `admin`

### 7. Run the ETL Pipeline

In the Airflow web interface, you can manually trigger the `wikipedia_flow` DAG:

1. Navigate to the `DAGs` tab.
2. Find the `wikipedia_flow` DAG and toggle the switch to "on".
3. Click on the `wikipedia_flow` DAG name to view the tasks.
4. Click the play button to trigger the DAG and ensure all tasks run successfully (turn green).

### 8. Test the ETL Pipeline

The pipeline should fetch data from Wikipedia, process it, and store it in Azure Storage. You can verify this by checking the output in your Azure Storage account.

## Troubleshooting

If you encounter any issues:

- Ensure Docker Desktop is running.
- Verify that your API keys are correct and properly set in the `.env` file.
- Check the logs in the Airflow web interface for detailed error messages.

## Contributing

Contributions are welcome! Please create an issue or submit a pull request for any improvements.

## License

This project is licensed under the MIT License.
