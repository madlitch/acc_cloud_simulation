import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

project_id = 'astute-lyceum-418705'
bucket_name = 'highdata_project'
dataset_id = 'highd'
table_id = 'acc_challenging_scenarios'


class ParseCsvData(beam.DoFn):
    def process(self, element):
        # split the CSV line into a list. Assumes comma as delimiter.
        values = element.split(',')

        # convert each field to the correct type. If field is empty, set it to None.
        try:
            parsed_values = {
                'frame': int(values[0]) if values[0] else None,
                'id': int(values[1]) if values[1] else None,
                'x': float(values[2]) if values[2] else None,
                'y': float(values[3]) if values[3] else None,
                'width': float(values[4]) if values[4] else None,
                'height': float(values[5]) if values[5] else None,
                'xVelocity': float(values[6]) if values[6] else None,
                'yVelocity': float(values[7]) if values[7] else None,
                'xAcceleration': float(values[8]) if values[8] else None,
                'yAcceleration': float(values[9]) if values[9] else None,
                'frontSightDistance': float(values[10]) if values[10] else None,
                'backSightDistance': float(values[11]) if values[11] else None,
                'dhw': float(values[12]) if values[12] else None,
                'thw': float(values[13]) if values[13] else None,
                'ttc': float(values[14]) if values[14] else None,
                'precedingXVelocity': float(values[15]) if values[15] else None,
                'precedingId': int(values[16]) if values[16] else None,
                'followingId': int(values[17]) if values[17] else None,
                'leftPrecedingId': int(values[18]) if values[18] else None,
                'leftAlongsideId': int(values[19]) if values[19] else None,
                'leftFollowingId': int(values[20]) if values[20] else None,
                'rightPrecedingId': int(values[21]) if values[21] else None,
                'rightAlongsideId': int(values[22]) if values[22] else None,
                'rightFollowingId': int(values[23]) if values[23] else None,
                'laneId': int(values[24]) if values[24] else None
            }
        except IndexError:
            # handle case where CSV row is shorter than expected
            yield {}
            return

        yield parsed_values


class FilterData(beam.DoFn):
    def process(self, element):
        unsafe_dhw_threshold = 10  # unsafe distance headway in meters
        unsafe_thw_threshold = 3  # unsafe time headway in seconds
        high_relative_velocity_threshold = 5  # high relative velocity in m/s
        unsafe_acceleration_threshold = -2  # unsafe deceleration in m/s^2

        row = dict(element)

        # skip rows where the precedingId is 0 (no vehicle ahead)
        if int(row['precedingId']) == 0:
            return

        # apply filtering criteria
        # compute the relative velocity along the x-axis
        relative_velocity_x = abs(float(row['xVelocity']) - float(row['precedingXVelocity']))

        # check if the vehicle is following too closely based on frontSightDistance
        following_too_close = float(row['dhw']) <= unsafe_dhw_threshold or float(row['thw']) <= unsafe_thw_threshold

        # check if the relative velocity along the x-axis is higher than the threshold
        high_relative_velocity = relative_velocity_x >= high_relative_velocity_threshold

        # check if the acceleration indicates a strong deceleration
        strong_deceleration = float(row['xAcceleration']) < unsafe_acceleration_threshold

        # yield the row if any of the conditions are met
        if following_too_close or high_relative_velocity or strong_deceleration:
            yield row


def run():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        temp_location=f'gs://{bucket_name}/temp',
        region='northamerica-northeast2'
    )

    table_schema = {
        'fields': [
            {'name': 'frame', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'x', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'y', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'width', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'height', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'xVelocity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'yVelocity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'xAcceleration', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'yAcceleration', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'frontSightDistance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'backSightDistance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'dhw', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'thw', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ttc', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'precedingXVelocity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'precedingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'followingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'leftPrecedingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'leftAlongsideId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'leftFollowingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'rightPrecedingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'rightAlongsideId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'rightFollowingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'laneId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        # read tracks files from GCS
        file_pattern = f'gs://{bucket_name}/data/*_tracks.csv'
        lines = p | 'ReadFromGCS' >> ReadFromText(file_pattern, skip_header_lines=1)

        # parse CSV lines and then filter data
        filtered_data = (lines
                         | 'ParseCSV' >> beam.ParDo(ParseCsvData())
                         | 'Filter' >> beam.ParDo(FilterData()))

        # write the filtered data to BigQuery
        filtered_data | 'WriteToBigQuery' >> WriteToBigQuery(
            f'{project_id}:{dataset_id}.{table_id}',
            schema=table_schema,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    run()
