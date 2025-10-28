import json
import boto3
import requests
from typing import List, Dict, Set, Tuple
from datetime import datetime, date
import uuid
from dateutil import parser

from ..logger import log

class SoleraCorrectionsManager:
    """
    Manager for sending correction events to Solera API to erase data within a given time range
    """

    def __init__(self, partner_credentials_secret_name: str):
        self.partner_credentials_secret_name = partner_credentials_secret_name
        self.s3 = boto3.client('s3')

    def process_month_corrections(self, s3_bucket: str, year: int, month: int) -> Dict:
        """
        Process all Solera files for a given month and send correction events
        
        Args:
            s3_bucket: S3 bucket containing Solera files
            year: Year to process (e.g., 2024)
            month: Month to process (1-12)
            
        Returns:
            Dictionary with processing results
        """
        
        log.info(f"Starting correction processing for {year}/{month:02d}")
        
        # Get all reference IDs from files in the specified month
        reference_data = self._extract_reference_ids_from_month(s3_bucket, year, month)
        
        if not reference_data:
            log.info(f"No reference IDs found for {year}/{month:02d}")
            return {
                "status": "success",
                "message": "No reference IDs found",
                "total_corrections": 0,
                "successful_corrections": 0,
                "failed_corrections": 0
            }
        
        log.info(f"Found {len(reference_data)} unique reference IDs to correct")
        
        # Send correction events for each reference ID
        return self._send_correction_events(reference_data)

    def _extract_reference_ids_from_month(self, s3_bucket: str, year: int, month: int) -> Set[Tuple[str, str, str, str]]:
        """
        Extract all unique reference IDs from Solera files in the specified month
        
        Returns:
            Set of tuples: (referenceId, enrollmentId, programId, timestamp)
        """
        
        # Construct S3 prefix for the month
        s3_prefix = f"solera/{year}/{month:02d}/"
        
        log.info(f"Searching for files with prefix: {s3_prefix}")
        
        reference_data = set()
        
        try:
            # List all objects in the month directory
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
            
            file_count = 0
            for page in pages:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    
                    # Skip directories and non-JSONL files
                    if s3_key.endswith('/') or not s3_key.endswith('.jsonl'):
                        continue
                    
                    file_count += 1
                    log.info(f"Processing file {file_count}: {s3_key}")
                    
                    # Process this file
                    file_reference_data = self._extract_reference_ids_from_file(s3_bucket, s3_key)
                    reference_data.update(file_reference_data)
            
            log.info(f"Processed {file_count} files, found {len(reference_data)} unique reference IDs")
            
        except Exception as e:
            log.error(f"Error listing S3 objects: {str(e)}")
            raise
        
        return reference_data

    def _extract_reference_ids_from_file(self, s3_bucket: str, s3_key: str) -> Set[Tuple[str, str, str, str]]:
        """
        Extract reference IDs from a single JSONL file
        
        Returns:
            Set of tuples: (referenceId, enrollmentId, programId, timestamp)
        """
        
        reference_data = set()
        
        try:
            # Download the file
            response = self.s3.get_object(Bucket=s3_bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            
            if not content.strip():
                log.info(f"Empty file: {s3_key}")
                return reference_data
            
            # Parse JSONL content (each line is a separate JSON object)
            lines = content.strip().split('\n')
            
            for line_num, line in enumerate(lines, 1):
                if not line.strip():
                    continue
                    
                try:
                    user_data = json.loads(line)
                    activities = user_data.get('activities', [])
                    
                    for activity in activities:
                        reference_id = activity.get('referenceId')
                        enrollment_id = activity.get('enrollmentId')
                        program_id = activity.get('programId')
                        timestamp = activity.get('timestamp')
                        
                        if reference_id and enrollment_id and program_id and timestamp:
                            reference_data.add((reference_id, enrollment_id, program_id, timestamp))
                        else:
                            log.warning(f"Missing required fields in {s3_key} line {line_num}")
                    
                except json.JSONDecodeError as e:
                    log.error(f"JSON decode error in {s3_key} line {line_num}: {str(e)}")
                    continue
            
            log.info(f"Extracted {len(reference_data)} reference IDs from {s3_key}")
            
        except Exception as e:
            log.error(f"Error processing file {s3_key}: {str(e)}")
            raise
        
        return reference_data

    def _send_correction_events(self, reference_data: Set[Tuple[str, str, str, str]]) -> Dict:
        """
        Send correction events for all reference IDs
        
        Args:
            reference_data: Set of tuples (referenceId, enrollmentId, programId, timestamp)
            
        Returns:
            Dictionary with results
        """
        
        log.info(f"Sending correction events for {len(reference_data)} reference IDs")
        
        # Get API credentials
        try:
            api_credentials = self._get_partner_credentials()
        except Exception as e:
            log.error(f"Failed to get API credentials: {str(e)}")
            return {
                "status": "failed",
                "error": f"Failed to get API credentials: {str(e)}",
                "total_corrections": len(reference_data),
                "successful_corrections": 0,
                "failed_corrections": len(reference_data)
            }
        
        successful_corrections = 0
        failed_corrections = 0
        errors = []
        
        # Group reference IDs by enrollment/program for batching
        batches = self._group_references_for_batching(reference_data)
        
        for batch_num, (enrollment_id, program_id, references) in enumerate(batches, 1):
            log.info(f"Processing batch {batch_num}/{len(batches)}: {len(references)} corrections for enrollment {enrollment_id}")
            
            # Create correction activities for this batch
            activities = []
            for reference_id, original_timestamp in references:
                correction_activity = {
                    "eventType": "Correction",
                    "enrollmentId": enrollment_id,
                    "referenceId": reference_id,
                    "programId": program_id,
                    "timestamp": original_timestamp,  # Use original timestamp
                    "data": {}
                }
                activities.append(correction_activity)
            
            # Send this batch
            batch_result = self._send_correction_batch(activities, api_credentials, batch_num)
            
            if batch_result['success']:
                successful_corrections += len(references)
                log.info(f"Batch {batch_num} successful: {len(references)} corrections sent")
            else:
                failed_corrections += len(references)
                errors.append(f"Batch {batch_num}: {batch_result.get('error', 'Unknown error')}")
                log.error(f"Batch {batch_num} failed: {batch_result.get('error', 'Unknown error')}")
        
        # Determine overall status
        if failed_corrections == 0:
            status = "success"
            log.info(f"All {successful_corrections} correction events sent successfully")
        elif successful_corrections == 0:
            status = "failed"
            log.error(f"All {failed_corrections} correction events failed")
        else:
            status = "partial_success"
            log.warning(f"{successful_corrections} corrections succeeded, {failed_corrections} failed")
        
        return {
            "status": status,
            "total_corrections": len(reference_data),
            "successful_corrections": successful_corrections,
            "failed_corrections": failed_corrections,
            "batches_processed": len(batches),
            "errors": errors
        }

    def _group_references_for_batching(self, reference_data: Set[Tuple[str, str, str, str]]) -> List[Tuple[str, str, List[Tuple[str, str]]]]:
        """
        Group reference IDs by enrollment/program ID for efficient batching
        
        Returns:
            List of tuples: (enrollmentId, programId, [(referenceId, timestamp), ...])
        """
        
        grouped = {}
        
        for reference_id, enrollment_id, program_id, timestamp in reference_data:
            key = (enrollment_id, program_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append((reference_id, timestamp))
        
        # Convert to list format
        batches = []
        for (enrollment_id, program_id), references in grouped.items():
            batches.append((enrollment_id, program_id, references))
        
        log.info(f"Grouped {len(reference_data)} references into {len(batches)} batches")
        return batches

    def _send_correction_batch(self, activities: List[Dict], api_credentials: Dict, batch_num: int) -> Dict:
        """
        Send a batch of correction activities to Solera API
        """
        
        api_url = api_credentials.get('api_delivery_url')
        access_token = api_credentials.get('access_token')
        token_type = api_credentials.get('token_type', 'Bearer')
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'{token_type} {access_token}'
        }
        
        payload = {
            "activities": activities
        }
        
        try:
            log.info(f"Sending batch {batch_num} with {len(activities)} correction activities to Solera API...")
            
            response = requests.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=300
            )
            
            # Accept both 200 (OK) and 202 (Accepted) as success
            if response.status_code in [200, 202]:
                log.info(f"Batch {batch_num} successfully sent to Solera API (status: {response.status_code})")
                return {
                    'success': True,
                    'status_code': response.status_code,
                    'response': response.json() if response.content else {}
                }
            else:
                error_msg = f'API returned status {response.status_code}: {response.text}'
                log.error(f"Batch {batch_num} failed: {error_msg}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'error': error_msg
                }
                
        except requests.exceptions.RequestException as e:
            error_msg = f'Request failed: {str(e)}'
            log.error(f"Batch {batch_num} request failed: {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }

    def _get_partner_credentials(self) -> Dict:
        """Get OAuth access token from Solera's Auth0 endpoint"""
        
        secrets_client = boto3.client('secretsmanager')
        
        try:
            response = secrets_client.get_secret_value(
                SecretId=self.partner_credentials_secret_name
            )
            secret_value = response['SecretString']
            oauth_credentials = json.loads(secret_value)
            
            # Auth0 OAuth token request
            auth_url = oauth_credentials.get('api_auth_url')
            auth_payload = {
                "client_id": oauth_credentials.get('client_id'),
                "client_secret": oauth_credentials.get('client_secret'),
                "audience": oauth_credentials.get('audience'),
                "grant_type": "client_credentials"
            }
            api_delivery_url = oauth_credentials.get('api_delivery_url')
            
            auth_headers = {
                'Content-Type': 'application/json'
            }
            
            log.info("Requesting access token from Solera Auth0...")
            auth_response = requests.post(
                auth_url,
                json=auth_payload,
                headers=auth_headers,
                timeout=60
            )
            
            if auth_response.status_code == 200:
                token_data = auth_response.json()
                access_token = token_data.get('access_token')
                
                if not access_token:
                    raise ValueError("No access_token in Auth0 response")
                
                log.info("Successfully obtained access token from Solera Auth0")
                
                return {
                    'api_delivery_url': api_delivery_url,
                    'access_token': access_token,
                    'token_type': token_data.get('token_type', 'Bearer')
                }
            else:
                error_msg = f"Auth0 token request failed with status {auth_response.status_code}: {auth_response.text}"
                log.error(error_msg)
                raise ValueError(error_msg)
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to get access token from Auth0: {str(e)}"
            log.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Failed to retrieve OAuth credentials: {str(e)}"
            log.error(error_msg)
            raise ValueError(error_msg)


def main():
    """
    Example usage of the SoleraCorrectionsManager
    """
    
    # Configuration
    S3_BUCKET = "your-s3-bucket-name"
    PARTNER_CREDENTIALS_SECRET = "your-secret-name"
    YEAR = 2024
    MONTH = 9  # September
    
    try:
        # Initialize the corrections manager
        corrections_manager = SoleraCorrectionsManager(PARTNER_CREDENTIALS_SECRET)
        
        # Process corrections for the specified month
        result = corrections_manager.process_month_corrections(S3_BUCKET, YEAR, MONTH)
        
        # Print results
        print(f"Correction processing completed:")
        print(f"Status: {result['status']}")
        print(f"Total corrections: {result['total_corrections']}")
        print(f"Successful: {result['successful_corrections']}")
        print(f"Failed: {result['failed_corrections']}")
        
        if result.get('errors'):
            print(f"Errors: {result['errors']}")
            
    except Exception as e:
        print(f"Error running corrections: {str(e)}")
        raise


if __name__ == "__main__":
    main()