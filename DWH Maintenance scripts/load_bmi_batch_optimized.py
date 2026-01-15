import mysql.connector
import time
from datetime import datetime
from config import get_db_config

def connect_to_db():
    """Connect to the database using config"""
    config = get_db_config()
    return mysql.connector.connect(**config)

def process_bmi_batch_optimized(cursor, user_batch):
    """Process BMI calculation for a batch of users using optimized approach"""
    
    if not user_batch:
        return 0
    
    # Create a single query to get all weight and height data for the batch
    placeholders = ','.join(['%s'] * len(user_batch))
    
    # Get all weight records for this batch - using new schema
    cursor.execute(f"""
        SELECT user_id, value as weight_kg, effective_date, intake, source
        FROM body_weight_values_cleaned
        WHERE user_id IN ({placeholders})
        AND value IS NOT NULL 
        AND effective_date IS NOT NULL
        ORDER BY user_id, effective_date
    """, user_batch)
    
    weight_data = {}
    for row in cursor.fetchall():
        user_id = row[0]
        if user_id not in weight_data:
            weight_data[user_id] = []
        weight_data[user_id].append({
            'weight_kg': row[1],
            'effective_date': row[2],  # This is now datetime from new schema
            'intake': row[3],          # This is now datetime(3) from new schema  
            'source': row[4]
        })
    
    # Get all height records for this batch
    cursor.execute(f"""
        SELECT user_id, value as height_cm, effective_date
        FROM body_height_values
        WHERE user_id IN ({placeholders})
        AND value IS NOT NULL 
        AND value != 0
        ORDER BY user_id, effective_date
    """, user_batch)
    
    height_data = {}
    for row in cursor.fetchall():
        user_id = row[0]
        if user_id not in height_data:
            height_data[user_id] = []
        height_data[user_id].append({
            'height_cm': row[1],
            'effective_date': row[2]
        })
    
    # Prepare batch insert data
    bmi_records = []
    
    # Process each user's data in memory (much faster than DB queries)
    for user_id in user_batch:
        if user_id not in weight_data or user_id not in height_data:
            continue
        
        user_weights = weight_data[user_id]
        user_heights = height_data[user_id]
        
        # For each weight record, find closest height record
        for weight_record in user_weights:
            weight_effective = weight_record['effective_date']
            closest_height = None
            min_diff = float('inf')
            
            # Calculate differences in Python (much faster than DB queries)
            for height_record in user_heights:
                height_effective = height_record['effective_date']
                
                # Handle datetime vs date comparison
                if hasattr(weight_effective, 'date') and hasattr(height_effective, 'date'):
                    # Both are datetime objects
                    diff = abs((weight_effective.date() - height_effective.date()).days)
                elif hasattr(weight_effective, 'date'):
                    # weight is datetime, height is date
                    diff = abs((weight_effective.date() - height_effective).days)
                elif hasattr(height_effective, 'date'):
                    # weight is date, height is datetime  
                    diff = abs((weight_effective - height_effective.date()).days)
                else:
                    # Both are date objects
                    diff = abs((weight_effective - height_effective).days)
                
                if diff < min_diff:
                    min_diff = diff
                    closest_height = height_record['height_cm']
            
            if closest_height:
                # Calculate BMI
                bmi = weight_record['weight_kg'] / ((closest_height / 100) ** 2)
                
                # Add to batch insert - keeping all BMI table fields but using NULL/blank for missing ones
                bmi_records.append((
                    user_id,                          # user_id
                    bmi,                             # value (BMI calculation)
                    weight_record['effective_date'], # effective (use effective_date from new schema)
                    weight_record['effective_date'], # effective_date (use effective_date from new schema)
                    None,                            # effective_time (NULL - doesn't exist in new schema)
                    weight_record['intake'],         # intake (use intake from new schema)
                    weight_record['intake'],         # intake_date (use intake from new schema)  
                    None,                            # intake_time (NULL - doesn't exist in new schema)
                    weight_record['source']          # source
                ))
    
    # Batch insert all BMI records at once - keeping original BMI table schema
    if bmi_records:
        cursor.executemany("""
            INSERT IGNORE INTO bmi_values_cleaned
            (id, user_id, value, effective, effective_date, effective_time, 
             intake, intake_date, intake_time, source)
            VALUES (
                UUID_TO_BIN(UUID()), %s, %s, %s, 
                DATE(%s), %s, %s, 
                DATE(%s), %s, %s
            )
        """, bmi_records)
        
        return cursor.rowcount
    
    return 0

def get_user_batches(cursor, batch_size=1000, limit=None):
    """Get distinct user_ids in batches"""
    query = """
        SELECT DISTINCT user_id 
        FROM body_weight_values_cleaned 
        WHERE value IS NOT NULL AND effective_date IS NOT NULL
        ORDER BY user_id
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    users = [row[0] for row in cursor.fetchall()]
    print(f"📊 Found {len(users):,} unique users to process")
    
    # Split into batches
    for i in range(0, len(users), batch_size):
        yield users[i:i + batch_size]

def check_existing_records(cursor):
    """Check how many BMI records already exist"""
    cursor.execute("SELECT COUNT(*) as existing_count FROM bmi_values_cleaned")
    return cursor.fetchone()[0]

def main():
    """Main execution function"""
    script_start_time = time.time()
    
    try:
        print("🔗 Connecting to database...")
        conn = connect_to_db()
        cursor = conn.cursor()
        
        print("📊 Checking existing data...")
        existing_bmi_count = check_existing_records(cursor)
        
        print(f"  📈 Existing BMI records: {existing_bmi_count:,}")
        
        if existing_bmi_count > 0:
            response = input(f"⚠️  Found {existing_bmi_count:,} existing BMI records. Continue? (y/N): ")
            if response.lower() != 'y':
                print("❌ Aborted by user")
                return
        
        # Use larger batches since we're more efficient now
        batch_size = 1000  # Increased from 50
        total_processed = 0
        batch_count = 0
        total_records_inserted = 0
        
        # Remove test limit for full processing
        test_limit = None  # Set to 5000 for testing, None for full processing
        
        print(f"\n🚀 Starting optimized BMI calculation in batches of {batch_size} users...")
        
        for user_batch in get_user_batches(cursor, batch_size, test_limit):
            batch_count += 1
            batch_start_time = time.time()
            
            try:
                rows_inserted = process_bmi_batch_optimized(cursor, user_batch)
                conn.commit()
                
                batch_duration = time.time() - batch_start_time
                total_processed += len(user_batch)
                total_records_inserted += rows_inserted
                
                print(f"  ✅ Batch {batch_count}: {len(user_batch)} users, {rows_inserted} BMI records inserted ({batch_duration:.2f}s)")
                print(f"      📊 Total users processed: {total_processed:,}")
                print(f"      📊 Total BMI records created: {total_records_inserted:,}")
                
                # Calculate estimated time remaining
                if batch_count > 1:
                    avg_time_per_batch = (time.time() - script_start_time) / batch_count
                    remaining_batches = (len(list(get_user_batches(cursor, batch_size, test_limit))) - batch_count)
                    estimated_remaining = avg_time_per_batch * remaining_batches / 60  # minutes
                    if estimated_remaining > 0:
                        print(f"      ⏰ Estimated time remaining: {estimated_remaining:.1f} minutes")
                
            except Exception as e:
                print(f"  ❌ Batch {batch_count} failed: {e}")
                conn.rollback()
                break
        
        # Final verification
        print("\n📊 Final verification...")
        final_bmi_count = check_existing_records(cursor)
        new_records = final_bmi_count - existing_bmi_count
        
        print(f"  📈 Final BMI records: {final_bmi_count:,}")
        print(f"  ➕ New records created: {new_records:,}")
        
        if total_processed > 0:
            rate = total_records_inserted / total_processed
            print(f"  📊 Processing rate: {rate:.2f} BMI records per user")
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        
        total_duration = time.time() - script_start_time
        print(f"\n🏁 Total runtime: {total_duration:.2f}s ({total_duration/60:.1f} minutes)")

if __name__ == "__main__":
    main()