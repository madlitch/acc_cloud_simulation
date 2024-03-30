# ACC Cloud Simulation Workflow

This project demonstrates a comprehensive workflow for simulating Adaptive Cruise Control (ACC) behavior using real-world driving data in the HighD dataset.

### 1. Data Upload to Google Cloud Storage (GCS)

- The `highd` dataset is uploaded to a specified bucket in GCS.
- This dataset includes detailed driving data necessary for ACC simulation.


### 2. Data Parsing and Filtering with Google Dataflow

- A Dataflow job is created to read the dataset from GCS.
- The job parses the data to identify challenging scenarios based on predefined criteria (e.g. close proximity to another vehicle, high relative velocity, etc.).
- Identified scenarios are stored in a BigQuery table named `aac_challenging_scenarios`.

### 3. Publishing Challenging Scenarios to Google Pub/Sub

- The `publish_scenarios.py` script queries the `aac_challenging_scenarios` BigQuery table.
- Each scenario is published to the `simulate-scenario` Pub/Sub topic, triggering downstream processing.
- This step enables asynchronous, event-driven processing of scenarios.

### 4. Simulating ACC Behavior on Google Compute Engine (GCE)
- A GCE instance is set up with the necessary environment for running the ACC simulation, running the `simulate_scenarios.py` script.
- The instance subscribes to the `simulate-scenario` Pub/Sub topic and listens for new challenging scenarios.
- For each received scenario, the ACC behavior is simulated, and the outcome is stored in the `simulation_results` BigQuery table.

### 5. Visualization with Looker Studio
- A Looker Studio report is created to visualize the outcomes of the ACC simulations, using the `simulation_results` table as a data source.
- The report provides insights into how often the ACC system successfully avoided collisions, and other data.
- It allows for easy analysis of ACC performance under different driving conditions.
