# BMKG Weather Data Pipeline (Mulyorejo) 

A Data Engineering project that utilizes **Apache Airflow** and **Docker** to perform an Extract, Transform, Load (ETL) process on weather forecast data from the BMKG API in *real-time*.

##  Tech Stack
- **Orchestration**: Apache Airflow
- **Data Processing**: Python (Pandas)
- **Containerization**: Docker & Docker Compose
- **Data Source**: [BMKG Public API](https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=35.78.26.1001)

##  Pipeline Workflow
1. **Extract**: The `extract_bmkg_mulyorejo` task retrieves weather forecast data from the API periodically (every 5 minutes) and saves it as a raw `JSON` file.
2. **Transform & Load**: The `transform_bmkg_mulyorejo` task flattens the nested JSON structure, removes duplicates (*Deduplication*), and saves the clean data incrementally into a `CSV` table format.

##  How to Run
1. Ensure **Docker** and **Docker Compose** are installed on your machine.
2. Clone this repository and open a terminal/command prompt in its folder.
3. Run the following command to build the image and start the container:
   ```bash
   docker-compose up -d --build
   ```
4. Open your browser and access the Airflow UI at: `http://localhost:8080`.
5. Login using the username `admin`. *(Note: Because it uses the `airflow standalone` mode, the password is auto-generated and can typically be found in the `airflow/simple_auth_manager_passwords.json.generated` file or in your terminal logs).*
6. Unpause the `bmkg_mulyorejo_weather_dag` DAG.

##  Directory Structure
- `airflow/dags/` : Contains the Python scripts (DAGs) defining the ETL workflow.
- `data_bmkg_mulyorejo/` : Local storage for raw data (JSON) and cleaned data (CSV).
- `Dockerfile` & `docker-compose.yml` : Configuration files to run the Airflow environment.
