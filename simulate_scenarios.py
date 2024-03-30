from google.cloud import pubsub_v1, bigquery
from google.cloud.bigquery import SchemaField, Table
import json
import numpy as np
import signal

topic_id = 'simulate-scenario'
subscription_id = 'simulate-scenario-sub'
project_id = 'astute-lyceum-418705'
dataset_id = 'highd'
table_id = 'simulation_results'

# initialize Google Cloud clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
bigquery_client = bigquery.Client(project=project_id)


# create the simulation_results table
def ensure_bigquery_table_exists():
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        bigquery_client.get_table(table_ref)
        print("Table {} already exists.".format(table_id))
    except Exception as e:
        print("Table {} is not found. Creating now...".format(table_id))
        schema = [
            SchemaField("scenario_id", "INTEGER"),
            SchemaField("frame", "INTEGER"),
            SchemaField("collision_avoided", "BOOL"),
            SchemaField("max_relative_distance", "FLOAT"),
            SchemaField("min_relative_distance", "FLOAT"),
            SchemaField("min_ttc", "FLOAT"),
            SchemaField("average_velocity_acc", "FLOAT"),
            SchemaField("average_velocity_lead", "FLOAT"),
        ]
        table = Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


ensure_bigquery_table_exists()


class VehicleModel:
    def __init__(self, position, velocity):
        self.state = np.array([position, velocity], dtype=np.float32)

    def update_state(self, acceleration, dt):
        self.state[0] += self.state[1] * dt + 0.5 * acceleration * dt ** 2
        self.state[1] += acceleration * dt


def acc_controller(relative_distance, relative_velocity):
    desired_gap, kp, kd = 50.0, 0.5, 0.1
    acceleration_command = kp * (relative_distance - desired_gap) + kd * relative_velocity
    return acceleration_command


def calculate_ttc(relative_distance, relative_velocity):
    if relative_velocity < 0:
        return abs(relative_distance / relative_velocity)
    return np.inf  # return infinity if relative_velocity is positive or zero, indicating no collision risk


def simulate_acc_behavior(scenario_data, dt=0.1, total_time=10):
    lead_vehicle = VehicleModel(scenario_data['x'], scenario_data['xVelocity'])
    acc_vehicle = VehicleModel(scenario_data['x'] - scenario_data['frontSightDistance'],
                               scenario_data['precedingXVelocity'])

    ttc_values = []
    relative_distances = []
    velocities_acc = []
    velocities_lead = []

    safe_ttc_threshold = 2
    collision_avoided = True  # assume collision is avoided initially

    for time in np.arange(0, total_time + dt, dt):
        relative_distance = lead_vehicle.state[0] - acc_vehicle.state[0]
        relative_velocity = lead_vehicle.state[1] - acc_vehicle.state[1]
        acc_acceleration = acc_controller(relative_distance, relative_velocity)

        acc_vehicle.update_state(acc_acceleration, dt)

        ttc = calculate_ttc(relative_distance, relative_velocity)
        ttc_values.append(ttc)
        relative_distances.append(relative_distance)
        velocities_acc.append(acc_vehicle.state[1])
        velocities_lead.append(lead_vehicle.state[1])

        if ttc <= safe_ttc_threshold and ttc != np.inf:
            collision_avoided = False  # update if TTC indicates a potential collision

    max_relative_distance = float(max(relative_distances))
    min_relative_distance = float(min(relative_distances))
    min_ttc = min(ttc_values) if ttc_values else np.inf
    average_velocity_acc = float(np.mean(velocities_acc))
    average_velocity_lead = float(np.mean(velocities_lead))

    return {
        'frame': scenario_data['frame'],
        'collision_avoided': collision_avoided,
        'max_relative_distance': max_relative_distance,
        'min_relative_distance': min_relative_distance,
        'min_ttc': None if min_ttc == np.inf else float(min_ttc),
        'average_velocity_acc': average_velocity_acc,
        'average_velocity_lead': average_velocity_lead
    }


# callback Function for Pub/Sub Messages
def callback(message):
    print(f"Received message: {message}")
    scenario_data = json.loads(message.data)

    simulation_result = simulate_acc_behavior(scenario_data)

    # insert results into BigQuery
    rows_to_insert = [{'scenario_id': scenario_data['id'], **simulation_result}]
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

    if not errors:
        print(f"Frame {scenario_data['frame']} simulation success, new rows added.")
    else:
        print("Encountered errors while inserting rows:", errors)

    message.ack()


# subscribe and listen for messages
def listen():
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    # wrap the subscriber in a 'with' block to automatically call close() when done
    with subscriber:
        try:
            # when timeout is unspecified, the result method waits indefinitely.
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()  # trigger the shutdown
            streaming_pull_future.result()  # block until the shutdown is complete
        except Exception as e:
            streaming_pull_future.cancel()  # trigger the shutdown on other exceptions
            print(f"An exception occurred: {e}")
            raise


if __name__ == "__main__":
    def handler(signum, frame):
        print("Signal handler called with signal", signum)
        raise KeyboardInterrupt


    # listen to SIGINT (Ctrl-C) and SIGTERM (termination signal from system)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    # start listening for messages
    listen()
