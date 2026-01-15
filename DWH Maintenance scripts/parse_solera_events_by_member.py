import boto3
import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set

def parse_json_lines(content: str) -> List[dict]:
    """Parse JSONL content - one JSON object per line"""
    json_objects = []
    
    lines = content.strip().split('\n')
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
            
        try:
            json_obj = json.loads(line)
            json_objects.append(json_obj)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON on line {line_num}: {e}")
            continue
    
    return json_objects

def extract_month_from_uri(s3_uri: str) -> str:
    """Extract month from S3 URI"""
    parts = s3_uri.rstrip('/').split('/')
    if len(parts) >= 2:
        year = parts[-2]
        month = parts[-1]
        return f"{year}-{month}"
    return "unknown"

def collect_member_events(s3_uris: List[str]) -> tuple[Dict[str, Dict[str, int]], Dict[str, List[dict]]]:
    """
    Collect event counts and detailed event data per member
    
    Returns:
        Tuple of (member_event_counts, member_detailed_events)
        - member_event_counts: user_id -> event_type -> count
        - member_detailed_events: user_id -> list of event details
    """
    session = boto3.Session(profile_name='9am-prod')
    s3_client = session.client('s3')
    
    # Structure: user_id -> event_type -> count
    member_event_counts = defaultdict(lambda: defaultdict(int))
    # Structure: user_id -> list of detailed events
    member_detailed_events = defaultdict(list)
    
    total_files_processed = 0
    total_json_objects = 0
    
    for s3_uri in s3_uris:
        month = extract_month_from_uri(s3_uri)
        print(f"\nProcessing {month} data...")
        
        bucket_name = s3_uri.replace('s3://', '').split('/')[0]
        prefix = '/'.join(s3_uri.replace('s3://', '').split('/')[1:])
        
        print(f"Processing bucket: {bucket_name}, prefix: {prefix}")
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            file_count = 0
            month_json_objects = 0
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.json') or key.endswith('.jsonl'):
                        file_count += 1
                        total_files_processed += 1
                        print(f"Processing file {file_count}: {key}")
                        
                        try:
                            response = s3_client.get_object(Bucket=bucket_name, Key=key)
                            content = response['Body'].read().decode('utf-8')
                            
                            json_objects = parse_json_lines(content)
                            month_json_objects += len(json_objects)
                            total_json_objects += len(json_objects)
                            print(f"  Found {len(json_objects)} JSON objects")
                            
                            for json_obj in json_objects:
                                user_id = json_obj.get('user_id')
                                activities = json_obj.get('activities', [])
                                
                                if user_id and activities:
                                    for activity in activities:
                                        data = activity.get('data', {})
                                        reference_id = activity.get('referenceId', '')
                                        timestamp = activity.get('timestamp', '')
                                        enrollment_id = activity.get('enrollmentId', '')
                                        program_id = activity.get('programId', '')
                                        
                                        for event_type, event_value in data.items():
                                            # Count the event
                                            member_event_counts[user_id][event_type] += 1
                                            
                                            # Store detailed event info
                                            member_detailed_events[user_id].append({
                                                'event_type': event_type,
                                                'reference_id': reference_id,
                                                'timestamp': timestamp,
                                                'enrollment_id': enrollment_id,
                                                'program_id': program_id,
                                                'event_value': event_value
                                            })
                        
                        except Exception as e:
                            print(f"Error processing file {key}: {e}")
            
            print(f"Completed {month}: {file_count} files, {month_json_objects} JSON objects")
                            
        except Exception as e:
            print(f"Error accessing S3 URI {s3_uri}: {e}")
    
    print(f"\nProcessing complete:")
    print(f"  Total files processed: {total_files_processed}")
    print(f"  Total JSON objects: {total_json_objects}")
    print(f"  Total unique members: {len(member_event_counts)}")
    
    return dict(member_event_counts), dict(member_detailed_events)

def create_member_event_dataframe(member_event_counts: Dict[str, Dict[str, int]]) -> pd.DataFrame:
    """Create DataFrame with member_id and event counts as columns"""
    
    # Get all unique event types
    all_event_types = set()
    for member_data in member_event_counts.values():
        all_event_types.update(member_data.keys())
    
    # Create rows for DataFrame
    data = []
    for member_id, event_counts in member_event_counts.items():
        row = {'member_id': member_id}
        
        # Add count for each event type (0 if member didn't have that event)
        for event_type in sorted(all_event_types):
            row[event_type] = event_counts.get(event_type, 0)
        
        # Calculate total events for this member
        row['total_events'] = sum(event_counts.values())
        
        data.append(row)
    
    df = pd.DataFrame(data)
    df = df.sort_values('total_events', ascending=False)
    
    return df

def create_detailed_worksheets(df: pd.DataFrame, member_detailed_events: Dict[str, List[dict]], output_file: str):
    """Create Excel file with summary sheet and detailed event sheets for top 10 users"""
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write summary sheet
        df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Get top 10 most active members
        top_10_members = df.head(10)['member_id'].tolist()
        
        # Create a sheet for each top member
        for idx, member_id in enumerate(top_10_members, 1):
            # Get this member's detailed events
            events = member_detailed_events.get(member_id, [])
            
            if not events:
                continue
            
            # Create detailed DataFrame for this member with each event on its own row
            detail_data = []
            for event in events:
                detail_data.append({
                    'Event Type': event['event_type'],
                    'Reference ID': event['reference_id'],
                    'Timestamp': event['timestamp'],
                    'Enrollment ID': event['enrollment_id'],
                    'Program ID': event['program_id'],
                    'Event Value': str(event['event_value'])
                })
            
            detail_df = pd.DataFrame(detail_data)
            
            # Sort by timestamp
            detail_df = detail_df.sort_values('Timestamp')
            
            # Create safe sheet name (Excel has 31 char limit)
            sheet_name = f"Top{idx}_{member_id[:20]}"
            
            # Write to Excel
            detail_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Format the worksheet
            worksheet = writer.sheets[sheet_name]
            worksheet.column_dimensions['A'].width = 30  # Event Type
            worksheet.column_dimensions['B'].width = 38  # Reference ID
            worksheet.column_dimensions['C'].width = 25  # Timestamp
            worksheet.column_dimensions['D'].width = 25  # Enrollment ID
            worksheet.column_dimensions['E'].width = 15  # Program ID
            worksheet.column_dimensions['F'].width = 40  # Event Value
            
            # Add member ID and event count as title
            worksheet.insert_rows(1, 2)
            worksheet['A1'] = f'Member ID: {member_id}'
            worksheet['A2'] = f'Total Events: {len(events)}'
            worksheet['A1'].font = worksheet['A1'].font.copy(bold=True, size=12)
            worksheet['A2'].font = worksheet['A2'].font.copy(bold=True, size=11)
        
        print(f"\nCreated detailed worksheets for top 10 members")

def create_single_detailed_csv(df: pd.DataFrame, member_detailed_events: Dict[str, List[dict]], output_file: str = "all_member_details.csv"):
    """Create single CSV with all member events"""
    
    all_events = []
    
    for member_id, events in member_detailed_events.items():
        for event in events:
            all_events.append({
                'Member ID': member_id,
                'Event Type': event['event_type'],
                'Reference ID': event['reference_id'],
                'Timestamp': event['timestamp'],
                'Enrollment ID': event['enrollment_id'],
                'Program ID': event['program_id'],
                'Event Value': str(event['event_value'])
            })
    
    # Create DataFrame and sort
    all_events_df = pd.DataFrame(all_events)
    all_events_df = all_events_df.sort_values(['Member ID', 'Timestamp'])
    
    # Save to CSV
    all_events_df.to_csv(output_file, index=False)
    
    print(f"\nSaved all member events to: {output_file}")
    print(f"Total events: {len(all_events)}")

def create_detailed_csv_files(df: pd.DataFrame, member_detailed_events: Dict[str, List[dict]], output_dir: str = "member_details"):
    """Create CSV files for all members in a directory"""
    import os
    
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save summary
    df.to_csv(f"{output_dir}/summary.csv", index=False)
    
    # Create CSV for each member
    all_members = df['member_id'].tolist()
    
    for idx, member_id in enumerate(all_members, 1):
        events = member_detailed_events.get(member_id, [])
        
        if not events:
            continue
        
        # Create detailed DataFrame
        detail_data = []
        for event in events:
            detail_data.append({
                'Member ID': member_id,
                'Event Type': event['event_type'],
                'Reference ID': event['reference_id'],
                'Timestamp': event['timestamp'],
                'Enrollment ID': event['enrollment_id'],
                'Program ID': event['program_id'],
                'Event Value': str(event['event_value'])
            })
        
        detail_df = pd.DataFrame(detail_data)
        detail_df = detail_df.sort_values('Timestamp')
        
        # Safe filename
        safe_member_id = "".join(c for c in member_id if c.isalnum() or c in ('-', '_'))[:50]
        filename = f"{output_dir}/member_{safe_member_id}.csv"
        
        detail_df.to_csv(filename, index=False)
        
        if idx % 100 == 0:
            print(f"Created CSV {idx}/{len(all_members)}")
    
    print(f"\nCreated CSV files for all {len(all_members)} members in '{output_dir}' directory")

def print_summary_stats(df: pd.DataFrame):
    """Print summary statistics"""
    print("\n" + "="*100)
    print("MEMBER EVENT SUMMARY STATISTICS")
    print("="*100)
    
    if df.empty:
        print("No data was processed.")
        return
    
    print(f"\nTotal Members: {len(df)}")
    print(f"Total Events Across All Members: {df['total_events'].sum()}")
    print(f"Average Events per Member: {df['total_events'].mean():.2f}")
    print(f"Median Events per Member: {df['total_events'].median():.0f}")
    
    print(f"\nTop 10 Most Active Members:")
    print("-" * 80)
    print(df[['member_id', 'total_events']].head(10).to_string(index=False))
    
    print(f"\nEvent Type Distribution:")
    print("-" * 80)
    event_cols = [col for col in df.columns if col not in ['member_id', 'total_events']]
    for event_col in event_cols:
        total = df[event_col].sum()
        members_with_event = (df[event_col] > 0).sum()
        print(f"{event_col:<35} Total: {total:>6}  Members: {members_with_event:>5}")

def main():
    s3_uris = [
        "s3://datawarehouseproduseast-datashar-datasharingbucket-bufcplwautfa/solera/2025/09/",
        "s3://datawarehouseproduseast-datashar-datasharingbucket-bufcplwautfa/solera/2025/10/",
        "s3://datawarehouseproduseast-datashar-datasharingbucket-bufcplwautfa/solera/2025/11/",
        "s3://datawarehouseproduseast-datashar-datasharingbucket-bufcplwautfa/solera/2025/12/",
        "s3://datawarehouseproduseast-datashar-datasharingbucket-bufcplwautfa/solera/2026/01/"
    ]
    
    print("Starting to process Solera events by member ID...")
    
    member_event_counts, member_detailed_events = collect_member_events(s3_uris)
    
    if not member_event_counts:
        print("No data collected!")
        return
    
    df = create_member_event_dataframe(member_event_counts)
    print_summary_stats(df)
    
    if not df.empty:
        # Create single CSV with all events from all members
        create_single_detailed_csv(df, member_detailed_events, "all_solera_member_events.csv")
        print("✅ CSV created with all member events!")
if __name__ == "__main__":
    main()