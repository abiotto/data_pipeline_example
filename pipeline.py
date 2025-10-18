import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import ReadFromMongoDB
from apache_beam.io.jdbc import ReadFromJdbc
import pyarrow as pa
import pyarrow.parquet as pq
import os
import uuid


class WriteToParquet(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, element):
        table = pa.Table.from_pylist([element])
        filename = f'{uuid.uuid4()}.parquet'
        pq.write_table(table, os.path.join(self.output_path, filename))
        yield filename


def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo_uri')
    parser.add_argument('--mongo_db')
    parser.add_argument('--mongo_collection')
    parser.add_argument('--pg_host')
    parser.add_argument('--pg_port')
    parser.add_argument('--pg_user')
    parser.add_argument('--pg_password')
    parser.add_argument('--pg_database')
    parser.add_argument('--pg_table')
    parser.add_argument('--output_path')
    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(beam_args, runner='DirectRunner')

    with beam.Pipeline(options=pipeline_options) as p:
        mongo_data = (
            p
            | 'Read from MongoDB' >> ReadFromMongoDB(
                uri=args.mongo_uri,
                db=args.mongo_db,
                coll=args.mongo_collection
            )
        )

        jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_database}"
        pg_data = (
            p
            | 'Read from Postgres' >> ReadFromJdbc(
                table_name=args.pg_table,
                jdbc_url=jdbc_url,
                driver_class_name='org.postgresql.Driver',
                username=args.pg_user,
                password=args.pg_password,
                classpath='/app/postgresql-42.7.3.jar'
            )
        )

        all_data = (mongo_data, pg_data) | beam.Flatten()

        all_data | 'Write to Parquet' >> beam.ParDo(WriteToParquet(args.output_path))


if __name__ == '__main__':
    run()
