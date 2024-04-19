import boto3
import csv
import time
import logging
import argparse
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("CSV file to Kinesis Data Streams")


class GlobalVariables():
    csv_file_path = './train.csv'
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')


def define_arguments():
    """
    Defines the command-line arguments
    """
    parser = argparse.ArgumentParser(description="Send CSV data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=float, required=True, help="Time interval (in seconds) between two writes")
    parser.add_argument("--max_rows", "-mr", type=int, help="Maximum number of rows to write (max: 150)")
    args = parser.parse_args()

    return args


def determine_partition_key(species):
    """
    Data will be partitioned due to species name
    """
    partition_key_mapping = {
        '0': '1',
        '1': '2'
    }
    return partition_key_mapping.get(species, 'unknown')


def send_csv_to_kinesis(stream_name, interval, max_rows, csv_file=GlobalVariables.csv_file_path):
    client = GlobalVariables.kinesis_client
    header_sent = False  # Flag to track if header has been sent

    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Read the header row
        header_str = ','.join(header)  # Convert header to string

        rows_written = 0
        for i, row in enumerate(csv_reader):
            data = ','.join(row)  # Join row values with commas
            encoded_data = f"{data}\n".encode('utf-8')  # Encode the data as bytes

            # Send header only once at the beginning
            if not header_sent:
                encoded_header = f"{header_str}\n".encode('utf-8')
                response = client.put_record(
                    StreamName=stream_name,
                    Data=encoded_header,
                    PartitionKey='header'  # Use 'header' as the partition key for the header row
                )
                logging.info(f"Header sent: {response['SequenceNumber']}")
                header_sent = True  # Mark header as sent

            response = client.put_record(
                StreamName=stream_name,
                Data=encoded_data,
                PartitionKey=str(i)  # Use row index as the partition key
            )
            logging.info(f"Record sent: {response['SequenceNumber']}")

            time.sleep(interval)

            rows_written += 1
            if rows_written >= max_rows:
                break





if __name__ == "__main__":
    args = define_arguments()

    send_csv_to_kinesis(args.stream_name, args.interval, args.max_rows)
