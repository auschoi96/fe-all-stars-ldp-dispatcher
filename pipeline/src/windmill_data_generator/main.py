import os
import time
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dbldatagen as dg

def create_windmill_data_spec(spark, num_records=10):
    """
    Create a dbldatagen DataGenerator specification for windmill IoT sensor data
    that indicates potential problems requiring maintenance.
    """
    
    # Define the schema for windmill sensor data
    windmill_schema = StructType([
        StructField("turbine_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("wind_speed_ms", DoubleType(), False),
        StructField("wind_direction_deg", DoubleType(), False),
        StructField("rotor_rpm", DoubleType(), False),
        StructField("power_output_kw", DoubleType(), False),
        StructField("nacelle_temp_c", DoubleType(), False),
        StructField("gearbox_temp_c", DoubleType(), False),
        StructField("generator_temp_c", DoubleType(), False),
        StructField("vibration_ms2", DoubleType(), False),
        StructField("oil_pressure_bar", DoubleType(), False),
        StructField("brake_pressure_bar", DoubleType(), False),
        StructField("pitch_angle_deg", DoubleType(), False),
        StructField("yaw_angle_deg", DoubleType(), False),
        StructField("tower_lean_deg", DoubleType(), False),
        StructField("fault_code", StringType(), True),
        StructField("maintenance_needed", BooleanType(), False),
        StructField("alert_level", StringType(), False),
        StructField("location_lat", DoubleType(), False),
        StructField("location_lon", DoubleType(), False)
    ])
    
    # Create data generator
    data_generator = (
        dg.DataGenerator(spark, name="windmill_sensors", rows=num_records, partitions=4)
        .withIdOutput()
        .withColumn("turbine_id", "string", format="TURBINE_%05d", baseColumn="id")
        .withColumn("timestamp", "timestamp", 
                   begin=datetime.now() - timedelta(minutes=5),
                   end=datetime.now(),
                   interval=timedelta(seconds=30))
        
        # Wind conditions
        .withColumn("wind_speed_ms", "double", minValue=2.0, maxValue=25.0, 
                   random=True, distribution="normal")
        .withColumn("wind_direction_deg", "double", minValue=0.0, maxValue=360.0, random=True)
        
        # Turbine operation - correlated with wind speed
        .withColumn("rotor_rpm", "double", 
                   expr="case when wind_speed_ms < 3.5 then 0 " +
                        "when wind_speed_ms > 20 then 0 " +
                        "else 5 + (wind_speed_ms - 3.5) * 2.5 end + (rand() - 0.5) * 2")
        
        .withColumn("power_output_kw", "double",
                   expr="case when rotor_rpm = 0 then 0 " +
                        "else least(2000, rotor_rpm * rotor_rpm * 0.8) + (rand() - 0.5) * 50 end")
        
        # Temperature readings - some with problems
        .withColumn("nacelle_temp_c", "double", minValue=15.0, maxValue=85.0, 
                   random=True, distribution="normal")
        .withColumn("gearbox_temp_c", "double", 
                   expr="nacelle_temp_c + 15 + (rand() - 0.5) * 10 + " +
                        "case when rand() < 0.05 then 40 else 0 end")  # 5% chance of overheating
        .withColumn("generator_temp_c", "double",
                   expr="nacelle_temp_c + 10 + (rand() - 0.5) * 8 + " +
                        "case when rand() < 0.03 then 35 else 0 end")  # 3% chance of overheating
        
        # Vibration - higher values indicate bearing problems
        .withColumn("vibration_ms2", "double",
                   expr="0.5 + rotor_rpm * 0.02 + (rand() - 0.5) * 0.3 + " +
                        "case when rand() < 0.08 then rand() * 2.0 else 0 end")  # 8% chance of high vibration
        
        # Pressure systems
        .withColumn("oil_pressure_bar", "double",
                   expr="45 + (rand() - 0.5) * 8 + " +
                        "case when rand() < 0.06 then -(rand() * 15) else 0 end")  # 6% chance of low pressure
        .withColumn("brake_pressure_bar", "double", minValue=180.0, maxValue=220.0, 
                   random=True, distribution="normal")
        
        # Mechanical positioning
        .withColumn("pitch_angle_deg", "double",
                   expr="case when wind_speed_ms > 15 then 15 + (wind_speed_ms - 15) * 2 " +
                        "else (rand() - 0.5) * 5 end")
        .withColumn("yaw_angle_deg", "double", 
                   expr="wind_direction_deg + (rand() - 0.5) * 10")
        .withColumn("tower_lean_deg", "double",
                   expr="(rand() - 0.5) * 0.8 + " +
                        "case when rand() < 0.02 then rand() * 2.0 else 0 end")  # 2% chance of excessive lean
        
        # Location (wind farm in Texas)
        .withColumn("location_lat", "double", minValue=32.0, maxValue=32.1, random=True)
        .withColumn("location_lon", "double", minValue=-101.5, maxValue=-101.4, random=True)
        
        # Fault detection logic
        .withColumn("fault_code", "string",
                   expr="case " +
                        "when gearbox_temp_c > 80 then 'GEARBOX_OVERHEAT' " +
                        "when generator_temp_c > 75 then 'GENERATOR_OVERHEAT' " +
                        "when vibration_ms2 > 2.0 then 'HIGH_VIBRATION' " +
                        "when oil_pressure_bar < 35 then 'LOW_OIL_PRESSURE' " +
                        "when tower_lean_deg > 1.5 then 'TOWER_LEAN' " +
                        "when rotor_rpm > 0 and power_output_kw < 100 then 'LOW_POWER_OUTPUT' " +
                        "else null end")
        
        .withColumn("maintenance_needed", "boolean",
                   expr="fault_code is not null")
        
        .withColumn("alert_level", "string",
                   expr="case " +
                        "when fault_code in ('GEARBOX_OVERHEAT', 'GENERATOR_OVERHEAT', 'TOWER_LEAN') then 'CRITICAL' " +
                        "when fault_code in ('HIGH_VIBRATION', 'LOW_OIL_PRESSURE') then 'WARNING' " +
                        "when fault_code is not null then 'INFO' " +
                        "else 'NORMAL' end")
    )
    
    return data_generator

def create_or_get_table(spark, catalog, schema, table_name):
    """Create the bronze table if it doesn't exist"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        turbine_id STRING,
        timestamp TIMESTAMP,
        wind_speed_ms DOUBLE,
        wind_direction_deg DOUBLE,
        rotor_rpm DOUBLE,
        power_output_kw DOUBLE,
        nacelle_temp_c DOUBLE,
        gearbox_temp_c DOUBLE,
        generator_temp_c DOUBLE,
        vibration_ms2 DOUBLE,
        oil_pressure_bar DOUBLE,
        brake_pressure_bar DOUBLE,
        pitch_angle_deg DOUBLE,
        yaw_angle_deg DOUBLE,
        tower_lean_deg DOUBLE,
        fault_code STRING,
        maintenance_needed BOOLEAN,
        alert_level STRING,
        location_lat DOUBLE,
        location_lon DOUBLE,
        ingestion_timestamp TIMESTAMP,
        batch_id STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    spark.sql(create_table_sql)
    return full_table_name

def insert_batch_data(spark, data_generator, table_name, batch_id):
    """Generate and insert a batch of data"""
    # Generate the data
    df = data_generator.build()
    
    # Drop the auto-generated id column and add metadata columns
    df_with_metadata = df.drop("id") \
                        .withColumn("ingestion_timestamp", current_timestamp()) \
                        .withColumn("batch_id", lit(batch_id))
    
    # Insert into table
    df_with_metadata.write.mode("append").saveAsTable(table_name)
    
    # Return count and summary
    record_count = df_with_metadata.count()
    fault_count = df_with_metadata.filter(col("fault_code").isNotNull()).count()
    
    return record_count, fault_count

def main():
    parser = argparse.ArgumentParser(description="Generate windmill IoT streaming data")
    parser.add_argument("--catalog", default="main", help="Unity Catalog name")
    parser.add_argument("--schema", default="windmill_iot", help="Schema name")
    parser.add_argument("--table", default="raw_sensor_data", help="Table name")
    parser.add_argument("--records-per-batch", type=int, default=10, help="Records per batch")
    parser.add_argument("--batch-interval", type=int, default=15, help="Seconds between batches")
    parser.add_argument("--max-iterations", type=int, default=100000, help="Maximum iterations")
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WindmillDataGenerator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Create or get table
    full_table_name = create_or_get_table(spark, args.catalog, args.schema, args.table)
    
    # Create data generator
    data_generator = create_windmill_data_spec(spark, args.records_per_batch)
    
    print(f"Starting windmill data generation...")
    print(f"Target table: {full_table_name}")
    print(f"Records per batch: {args.records_per_batch}")
    print(f"Batch interval: {args.batch_interval} seconds")
    print(f"Max iterations: {args.max_iterations}")
    
    total_records = 0
    total_faults = 0
    
    try:
        for iteration in range(args.max_iterations):
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{iteration:06d}"
            
            # Generate and insert data
            record_count, fault_count = insert_batch_data(
                spark, data_generator, full_table_name, batch_id
            )
            
            total_records += record_count
            total_faults += fault_count
            
            if iteration % 10 == 0:  # Log every 10 iterations
                print(f"Iteration {iteration:,}: Inserted {record_count} records, "
                      f"{fault_count} with faults. Total: {total_records:,} records, "
                      f"{total_faults:,} faults")
            
            # Wait for next batch
            if iteration < args.max_iterations - 1:
                time.sleep(args.batch_interval)
                
    except KeyboardInterrupt:
        print("\\nData generation stopped by user.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        print(f"\\nFinal summary:")
        print(f"Total records generated: {total_records:,}")
        print(f"Total fault records: {total_faults:,}")
        if total_records > 0:
            print(f"Fault rate: {(total_faults/total_records)*100:.2f}%")
        else:
            print("Fault rate: N/A (no records generated)")
        spark.stop()

if __name__ == "__main__":
    main()