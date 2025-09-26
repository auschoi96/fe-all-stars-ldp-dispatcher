import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Placeholder pipeline for future windmill data processing
# This will be expanded later with actual transformation logic

@dlt.table(
    name="bronze_sensor_data",
    comment="Raw windmill sensor data from external source table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_windmill_data():
    """
    Bronze layer: Read raw sensor data from the external table created by the data generation job
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    # Read from the raw_sensor_data table created by the data generator job
    # Use the pipeline target which resolves to users.{user_short_name}.raw_sensor_data
    return (
        spark
        .readStream
        .table(f"{spark.conf.get('pipelines.catalog')}.{spark.conf.get('pipelines.target')}.raw_sensor_data")
        .withColumnRenamed("timestamp", "event_timestamp")
    )

@dlt.table(
    name="silver_sensor_data_cleaned",
    comment="Cleaned and validated windmill sensor data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_turbine_id", "turbine_id IS NOT NULL AND turbine_id != ''")
@dlt.expect_or_drop("valid_timestamp", "ingestion_timestamp IS NOT NULL")
@dlt.expect_or_drop("reasonable_wind_speed", "wind_speed_ms BETWEEN 0 AND 50")
@dlt.expect_or_drop("valid_temperatures", "nacelle_temp_c BETWEEN -40 AND 120 AND gearbox_temp_c BETWEEN -40 AND 120 AND generator_temp_c BETWEEN -40 AND 120")
@dlt.expect_or_drop("valid_pressures", "oil_pressure_bar BETWEEN 0 AND 100 AND brake_pressure_bar BETWEEN 0 AND 300")
@dlt.expect_or_drop("valid_vibration", "vibration_ms2 >= 0 AND vibration_ms2 <= 10")
@dlt.expect_or_drop("valid_coordinates", "location_lat BETWEEN -90 AND 90 AND location_lon BETWEEN -180 AND 180")
def silver_windmill_data():
    """
    Silver layer: Cleaned and validated sensor data
    - Data quality validation with expectations
    - Deduplication based on turbine_id and timestamp
    - Schema standardization and type casting
    - Enrichment with derived metrics and status indicators
    """
    
    # Read from bronze layer
    bronze_df = dlt.read_stream("bronze_sensor_data")
    
    return (
        bronze_df
        # Deduplication: Keep latest record per turbine_id and timestamp
        .withWatermark("ingestion_timestamp", "1 minutes")  # Handle late arriving data
        .dropDuplicates(["turbine_id", "ingestion_timestamp"])
        
        # Schema standardization and data cleansing
        .withColumn("wind_speed_ms", F.round(F.col("wind_speed_ms"), 2))
        .withColumn("power_output_kw", F.round(F.col("power_output_kw"), 1))
        .withColumn("rotor_rpm", F.round(F.col("rotor_rpm"), 1))
        
        # Normalize wind direction to 0-360 range
        .withColumn("wind_direction_deg", 
                   F.when(F.col("wind_direction_deg") < 0, 
                         F.col("wind_direction_deg") + 360)
                   .when(F.col("wind_direction_deg") >= 360, 
                         F.col("wind_direction_deg") - 360)
                   .otherwise(F.col("wind_direction_deg")))
        
        # Enrichment: Add derived metrics
        .withColumn("temperature_differential_c", 
                   F.col("gearbox_temp_c") - F.col("nacelle_temp_c"))
        
        .withColumn("power_efficiency_ratio",
                   F.when(F.col("rotor_rpm") > 0, 
                         F.col("power_output_kw") / F.col("rotor_rpm"))
                   .otherwise(F.lit(0.0)))
        
        .withColumn("operating_status",
                   F.when(F.col("rotor_rpm") <= 0, "STOPPED")
                   .when(F.col("wind_speed_ms") < 3.5, "IDLE")  
                   .when(F.col("wind_speed_ms") > 20, "CUT_OUT")
                   .otherwise("GENERATING"))
        
        # Enrichment: Enhanced fault categorization
        .withColumn("fault_category",
                   F.when(F.col("fault_code").rlike("TEMP|OVERHEAT"), "THERMAL")
                   .when(F.col("fault_code").rlike("VIBRATION"), "MECHANICAL")
                   .when(F.col("fault_code").rlike("PRESSURE"), "HYDRAULIC")
                   .when(F.col("fault_code").rlike("POWER|OUTPUT"), "ELECTRICAL")
                   .when(F.col("fault_code").rlike("LEAN|TOWER"), "STRUCTURAL")
                   .otherwise("NORMAL"))
        
        # Enrichment: Maintenance priority scoring (1-5 scale)
        .withColumn("maintenance_priority",
                   F.when(F.col("alert_level") == "CRITICAL", 5)
                   .when(F.col("alert_level") == "WARNING", 3)
                   .when(F.col("alert_level") == "INFO", 1)
                   .otherwise(0))
        
        # Enrichment: Operational health score (0-100)
        .withColumn("health_score",
                   F.greatest(F.lit(0), 
                     F.least(F.lit(100),
                       F.lit(100) 
                       - F.when(F.col("fault_code").isNotNull(), 
                               F.col("maintenance_priority") * 10).otherwise(0)
                       - F.when(F.col("vibration_ms2") > 1.5, 10).otherwise(0)
                       - F.when(F.col("temperature_differential_c") > 30, 15).otherwise(0)
                       - F.when(F.col("power_efficiency_ratio") < 20, 5).otherwise(0)
                     )))
        
        # Add processing metadata
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("data_source", F.lit("windmill_iot_sensors"))
        .withColumn("pipeline_version", F.lit("1.0"))
        
        # Standardize column order
        .select(
            "turbine_id",
            "event_timestamp",
            "processed_timestamp",
            "operating_status",
            "health_score",
            "wind_speed_ms",
            "wind_direction_deg", 
            "rotor_rpm",
            "power_output_kw",
            "power_efficiency_ratio",
            "nacelle_temp_c",
            "gearbox_temp_c", 
            "generator_temp_c",
            "temperature_differential_c",
            "vibration_ms2",
            "oil_pressure_bar",
            "brake_pressure_bar",
            "pitch_angle_deg",
            "yaw_angle_deg", 
            "tower_lean_deg",
            "fault_code",
            "fault_category",
            "maintenance_needed",
            "maintenance_priority", 
            "alert_level",
            "location_lat",
            "location_lon",
            "data_source",
            "pipeline_version",
            "ingestion_timestamp",
            "batch_id"
        )
    )

@dlt.table(
    name="gold_maintenance_alerts",
    comment="Aggregated maintenance alerts and recommendations with predictive indicators",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_maintenance_alerts():
    """
    Gold layer: Business-ready maintenance alerts and analytics
    - Alert aggregation by turbine and time windows
    - Maintenance priority scoring
    - Predictive maintenance indicators  
    - SLA and performance metrics
    """
    
    # Read from silver layer
    silver_df = dlt.read_stream("silver_sensor_data_cleaned")
    
    return (
        silver_df
        .withWatermark("ingestion_timestamp", "5 minutes")  # Use sensor timestamp, allow more late data
        
        # Create time windows for aggregation (15-minute windows)
        .groupBy(
            F.col("turbine_id"),
            F.window(F.col("ingestion_timestamp"), "3 minutes").alias("time_window"),
            F.col("fault_category"),
            F.col("alert_level")
        )
        .agg(
            # Alert aggregations
            F.count("*").alias("total_readings"),
            F.sum(F.when(F.col("maintenance_needed") == True, 1).otherwise(0)).alias("maintenance_alerts_count"),
            F.max("maintenance_priority").alias("max_priority"),
            F.avg("maintenance_priority").alias("avg_priority"),
            
            # Health and performance metrics
            F.avg("health_score").alias("avg_health_score"),
            F.min("health_score").alias("min_health_score"),
            F.avg("power_efficiency_ratio").alias("avg_power_efficiency"),
            F.sum("power_output_kw").alias("total_power_output_kw"),
            F.avg("power_output_kw").alias("avg_power_output_kw"),
            
            # Temperature trend indicators
            F.avg("temperature_differential_c").alias("avg_temp_differential"),
            F.max("gearbox_temp_c").alias("max_gearbox_temp"),
            F.max("generator_temp_c").alias("max_generator_temp"),
            F.stddev("gearbox_temp_c").alias("gearbox_temp_variance"),
            
            # Vibration trend analysis
            F.avg("vibration_ms2").alias("avg_vibration"),
            F.max("vibration_ms2").alias("max_vibration"),
            F.stddev("vibration_ms2").alias("vibration_variance"),
            
            # Operational metrics
            F.avg("wind_speed_ms").alias("avg_wind_speed"),
            F.avg("rotor_rpm").alias("avg_rotor_rpm"),
            F.count(F.when(F.col("operating_status") == "GENERATING", 1)).alias("generating_count"),
            F.count(F.when(F.col("operating_status") == "STOPPED", 1)).alias("stopped_count"),
            
            # Pressure system health
            F.avg("oil_pressure_bar").alias("avg_oil_pressure"),
            F.min("oil_pressure_bar").alias("min_oil_pressure"),
            
            # Collection metadata
            F.min("ingestion_timestamp").alias("window_start"),
            F.max("ingestion_timestamp").alias("window_end"),
            F.first("location_lat").alias("turbine_lat"),
            F.first("location_lon").alias("turbine_lon")
        )
        
        # Add business intelligence metrics
        .withColumn("window_duration_minutes", 
                   (F.col("window_end").cast("long") - F.col("window_start").cast("long")) / 60)
        
        # Predictive maintenance indicators
        .withColumn("degradation_risk_score",
                   F.greatest(F.lit(0), F.least(F.lit(100),
                     # Base risk from current health
                     (100 - F.col("avg_health_score")) * 0.4 +
                     # Temperature variance risk (high variance indicates instability)
                     F.when(F.col("gearbox_temp_variance") > 5, 20).otherwise(0) +
                     # Vibration trend risk
                     F.when(F.col("vibration_variance") > 0.3, 15).otherwise(0) +
                     # Performance degradation risk
                     F.when(F.col("avg_power_efficiency") < 15, 25).otherwise(0)
                   )))
        
        # SLA Performance Metrics (targeting 95% uptime)
        .withColumn("uptime_percentage", 
                   (F.col("generating_count") / F.col("total_readings")) * 100)
        .withColumn("sla_compliance", 
                   F.when(F.col("uptime_percentage") >= 95, "COMPLIANT")
                   .when(F.col("uptime_percentage") >= 90, "WARNING")
                   .otherwise("BREACH"))
        
        # Enhanced priority scoring for maintenance dispatch
        .withColumn("dispatch_priority_score",
                   F.when(F.col("max_priority") >= 5, 100)  # Critical faults
                   .when(F.col("degradation_risk_score") >= 70, 85)  # High degradation risk
                   .when(F.col("sla_compliance") == "BREACH", 80)  # SLA breach
                   .when(F.col("maintenance_alerts_count") >= 5, 75)  # Multiple alerts
                   .when(F.col("avg_health_score") < 50, 70)  # Poor health
                   .when(F.col("uptime_percentage") < 90, 65)  # Poor performance
                   .otherwise(F.greatest(
                       F.col("avg_priority") * 10,
                       F.col("degradation_risk_score") * 0.3
                   )))
        
        # Maintenance recommendation categories
        .withColumn("recommended_action",
                   F.when(F.col("dispatch_priority_score") >= 90, "IMMEDIATE_INTERVENTION")
                   .when(F.col("dispatch_priority_score") >= 75, "SCHEDULED_MAINTENANCE")
                   .when(F.col("dispatch_priority_score") >= 60, "MONITORING_REQUIRED")
                   .when(F.col("degradation_risk_score") >= 50, "PREDICTIVE_MAINTENANCE")
                   .otherwise("ROUTINE_INSPECTION"))
        
        # Time-to-maintenance estimation (in days)
        .withColumn("estimated_maintenance_window_days",
                   F.when(F.col("recommended_action") == "IMMEDIATE_INTERVENTION", 1)
                   .when(F.col("recommended_action") == "SCHEDULED_MAINTENANCE", 7)
                   .when(F.col("recommended_action") == "MONITORING_REQUIRED", 30)
                   .when(F.col("recommended_action") == "PREDICTIVE_MAINTENANCE", 60)
                   .otherwise(90))
        
        # Add processing timestamp
        .withColumn("alert_generated_timestamp", F.current_timestamp())
        
        # Select and organize final schema
        .select(
            "turbine_id",
            "window_start",
            "window_end", 
            "window_duration_minutes",
            "turbine_lat",
            "turbine_lon",
            
            # Alert metrics
            "total_readings",
            "maintenance_alerts_count",
            "fault_category",
            "alert_level",
            "max_priority",
            "avg_priority",
            
            # Health and performance
            "avg_health_score",
            "min_health_score", 
            "avg_power_efficiency",
            "total_power_output_kw",
            "avg_power_output_kw",
            
            # SLA metrics
            "uptime_percentage", 
            "sla_compliance",
            "generating_count",
            "stopped_count",
            
            # Predictive indicators
            "degradation_risk_score",
            "avg_temp_differential",
            "gearbox_temp_variance",
            "vibration_variance",
            "max_vibration",
            "min_oil_pressure",
            
            # Maintenance recommendations
            "dispatch_priority_score",
            "recommended_action", 
            "estimated_maintenance_window_days",
            
            # Operational context
            "avg_wind_speed",
            "avg_rotor_rpm",
            "alert_generated_timestamp"
        )
    )

# Additional materialized views for specialized business use cases

@dlt.table(
    name="sla_performance_dashboard",
    comment="Real-time SLA performance metrics and compliance tracking"
)
def sla_performance_dashboard():
    """
    Materialized view for SLA monitoring dashboard
    Shows current performance against service level agreements
    """
    return (
        dlt.read("gold_maintenance_alerts")
        .groupBy("turbine_id", "turbine_lat", "turbine_lon")
        .agg(
            # SLA Performance Metrics
            F.avg("uptime_percentage").alias("avg_uptime_percentage"),
            F.min("uptime_percentage").alias("min_uptime_percentage"), 
            F.count(F.when(F.col("sla_compliance") == "COMPLIANT", 1)).alias("compliant_periods"),
            F.count(F.when(F.col("sla_compliance") == "BREACH", 1)).alias("breach_periods"),
            F.count("*").alias("total_periods"),
            
            # Power Generation Performance
            F.avg("avg_power_output_kw").alias("avg_power_generation_kw"),
            F.sum("total_power_output_kw").alias("total_power_generated_kw"),
            F.avg("avg_power_efficiency").alias("avg_efficiency_ratio"),
            
            # Health Trends
            F.avg("avg_health_score").alias("overall_health_score"),
            F.avg("degradation_risk_score").alias("avg_degradation_risk"),
            
            # Latest status
            F.max("alert_generated_timestamp").alias("last_updated")
        )
        .withColumn("sla_compliance_rate", 
                   (F.col("compliant_periods") / F.col("total_periods")) * 100)
        .withColumn("breach_rate",
                   (F.col("breach_periods") / F.col("total_periods")) * 100)
        .withColumn("performance_tier",
                   F.when(F.col("sla_compliance_rate") >= 98, "EXCELLENT")
                   .when(F.col("sla_compliance_rate") >= 95, "GOOD")
                   .when(F.col("sla_compliance_rate") >= 90, "ACCEPTABLE")
                   .otherwise("POOR"))
    )

@dlt.table(
    name="maintenance_dispatch_queue",
    comment="Priority queue for maintenance crew dispatch with work estimates"
)
def maintenance_dispatch_queue():
    """
    Materialized view for maintenance dispatch operations
    Provides prioritized work queue with time and resource estimates
    """
    return (
        dlt.read("gold_maintenance_alerts")
        .filter(F.col("recommended_action") != "ROUTINE_INSPECTION")
        .withColumn("work_complexity_hours",
                   F.when(F.col("fault_category") == "THERMAL", 4)
                   .when(F.col("fault_category") == "MECHANICAL", 8) 
                   .when(F.col("fault_category") == "HYDRAULIC", 3)
                   .when(F.col("fault_category") == "ELECTRICAL", 6)
                   .when(F.col("fault_category") == "STRUCTURAL", 12)
                   .otherwise(2))
        .withColumn("crew_size_required",
                   F.when(F.col("recommended_action") == "IMMEDIATE_INTERVENTION", 3)
                   .when(F.col("fault_category") == "STRUCTURAL", 4)
                   .when(F.col("fault_category") == "MECHANICAL", 2)
                   .otherwise(1))
        .withColumn("equipment_required",
                   F.when(F.col("fault_category") == "THERMAL", "THERMAL_IMAGING,COOLING_SYSTEM")
                   .when(F.col("fault_category") == "MECHANICAL", "VIBRATION_ANALYZER,BEARING_TOOLS")
                   .when(F.col("fault_category") == "HYDRAULIC", "PRESSURE_TEST_KIT,PUMPS")
                   .when(F.col("fault_category") == "ELECTRICAL", "MULTIMETER,INSULATION_TESTER")
                   .when(F.col("fault_category") == "STRUCTURAL", "CRANE,STRUCTURAL_REPAIR_KIT")
                   .otherwise("BASIC_TOOLS"))
        .select(
            "turbine_id",
            "turbine_lat", 
            "turbine_lon",
            "window_start",
            "dispatch_priority_score",
            "recommended_action",
            "fault_category",
            "estimated_maintenance_window_days",
            "work_complexity_hours",
            "crew_size_required", 
            "equipment_required",
            "avg_health_score",
            "degradation_risk_score",
            "maintenance_alerts_count",
            "alert_generated_timestamp"
        )
        .orderBy(F.col("dispatch_priority_score").desc(), F.col("estimated_maintenance_window_days").asc())
    )

# Data quality expectations are now integrated into the silver_windmill_data() function
# Additional expectations can be added as decorators to individual table functions