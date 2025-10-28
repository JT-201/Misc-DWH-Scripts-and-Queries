import sys
import boto3
import datetime
import time
import logging
from datetime import timedelta
from botocore.exceptions import ClientError

# Constants
environment = 'PROD'  # must be one of 'DEV' or 'PROD'

batch_size = 50 

from_date = datetime.date(2025, 5, 31)  # e.g. 2025-01-01
to_date = datetime.date(2025, 7, 7)
skip_sftp_upload = False

if environment == 'DEV':
    aws_profile = '9am-dev'
    job_name = 'MemberLevelEventsJob-DataWareHouseDevUs'
    base_arguments = {
        '--ActiveMemberEventManagers': 'transcarent',
        '--ReferenceDate': 'None',
        '--FromTimestamp': 'None',
        '--ToTimestamp': 'None',
        '--SkipSftpUpload': 'True',
    }
elif environment == 'PROD':
    aws_profile = '9am-prod'
    job_name = 'MemberLevelEventsJob-DataWareHouseProdUsEast'
    base_arguments = {
        '--ActiveMemberEventManagers': 'transcarent',
        '--ReferenceDate': 'None',
        '--FromTimestamp': 'None',
        '--ToTimestamp': 'None',
        '--SkipSftpUpload': 'False',
    }
else:
    raise ValueError("Invalid environment")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

dates_to_process = [from_date + timedelta(days=i) for i in range((to_date - from_date).days + 1)]

session = boto3.Session(profile_name=aws_profile)
glue_client = session.client('glue')

for i in range(0, len(dates_to_process), batch_size):
    batch_dates = dates_to_process[i:i+batch_size]
    running_jobs = []
    for current_date in batch_dates:
        arguments = base_arguments.copy()
        from_timestamp = current_date.strftime('%Y-%m-%dT00:00:00')
        to_timestamp = (current_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00')
        output_file_suffix = current_date.strftime('%Y-%m-%d')

        arguments['--FromTimestamp'] = from_timestamp
        arguments['--ToTimestamp'] = to_timestamp
        arguments['--ReferenceDate'] = 'None'
        arguments['--SkipSftpUpload'] = 'false' if skip_sftp_upload else 'false'
        arguments['--OutputFileNameSuffix'] = output_file_suffix

        # Retry logic for ConcurrentRunsExceededException
        while True:
            try:
                response = glue_client.start_job_run(
                    JobName=job_name,
                    Arguments=arguments
                )
                job_run_id = response['JobRunId']
                start_time = datetime.datetime.now()
                logging.info(f"Started job {job_run_id} for from {from_timestamp} to {to_timestamp} at {start_time}")
                running_jobs.append((job_run_id, start_time, from_timestamp, to_timestamp))
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConcurrentRunsExceededException':
                    logging.warning(f"Concurrent runs exceeded. Waiting 10 seconds before retrying for from {from_timestamp} to {to_timestamp}.")
                    time.sleep(10)
                else:
                    logging.error(f"Failed to start job for from {from_timestamp} to {to_timestamp}: {e}")
                    break
            except Exception as e:
                logging.error(f"Failed to start job for from {from_timestamp} to {to_timestamp}: {e}")
                break

    # Wait for all jobs in the batch to complete before starting the next batch
    for job_run_id, start_time, from_timestamp, to_timestamp in running_jobs:
        while True:
            try:
                job_response = glue_client.get_job_run(
                    JobName=job_name,
                    RunId=job_run_id,
                    PredecessorsIncluded=False
                )
                job_run_state = job_response['JobRun']['JobRunState']
                if job_run_state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                    end_time = datetime.datetime.now()
                    duration = end_time - start_time
                    logging.info(f"Job {job_run_id} for from {from_timestamp} to {to_timestamp} finished with status {job_run_state} at {end_time}, duration: {duration}")
                    break
                else:
                    time.sleep(30)
            except Exception as e:
                logging.error(f"Failed to get status for job {job_run_id}: {e}")
                time.sleep(30)

    # Add a short delay to ensure Glue releases the slot
    time.sleep(10)

logging.info("All jobs have been completed.")