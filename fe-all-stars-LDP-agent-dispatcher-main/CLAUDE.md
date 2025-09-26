# FE All Stars Agent Dispatcher - Project Requirements and Progress (PRP)

## Project Overview
**Name**: FE All Stars Agent Dispatcher
**Purpose**: Real-time windmill IoT monitoring dashboard with Databricks integration
**Architecture**: React frontend + FastAPI backend + Databricks Asset Bundle (DAB)

## Key Components

### 1. Data Pipeline (Databricks)
- **Windmill Data Generator Job**: Generates synthetic IoT sensor data (`raw_sensor_data` table)
- **DLT Pipeline**: Bronze � Silver � Gold architecture for windmill data processing
- **Event Log Publishing**: Enabled for monitoring incremental refreshes
- **Unity Catalog Integration**: `users.justin_edrington` schema

### 2. Frontend (React + TypeScript)
- **Single-page application** (no routing)
- **Windmill icon** in header (replaced chart icon)
- **Real-time monitoring** of Jobs & Pipelines, Active/Idle states
- **Optimized dependencies**: Reduced from 10,000+ to 93 packages
- **Performance optimized**: Removed Tables and Data Assets sections

### 3. Backend (FastAPI)
- **Databricks SDK integration** with timeout handling
- **Pipeline and jobs API endpoints** with error fallbacks
- **Async executors** for synchronous SDK calls

## Configuration Files

### Databricks Bundle (`/pipeline/databricks.yml`)
```yaml
bundle:
  name: windmill-iot-monitoring

resources:
  jobs:
    windmill_data_generator:
      name: "windmill-data-generator-${bundle.environment}"
      job_clusters:
        - job_cluster_key: "main"
          new_cluster:
            data_security_mode: "SINGLE_USER"
            runtime_engine: "PHOTON"
            node_type_id: "i3.xlarge"
            num_workers: 0
            spark_conf:
              "spark.databricks.cluster.profile": "singleNode"
              "spark.master": "local[*]"

  pipelines:
    windmill_pipeline:
      name: "windmill-iot-pipeline-${bundle.environment}"
      catalog: "users"
      target: "justin_edrington"
      configuration:
        "pipelines.enableEventLogging": "true"
        "raw_sensor_data_table": "raw_sensor_data"
      clusters:
        - label: "default"
          data_security_mode: "SINGLE_USER"
          runtime_engine: "PHOTON"
          node_type_id: "i3.xlarge"
          num_workers: 0
```

### Environment Configuration
- **Profile**: `e2-demo-field-eng`
- **Catalog**: `users`
- **Schema**: `justin_edrington`

## Development Commands

### Deployment
```bash
cd pipeline && databricks bundle deploy --profile e2-demo-field-eng
```

### Start Jobs
```bash
# Data Generator
databricks bundle run windmill_data_generator --profile e2-demo-field-eng

# DLT Pipeline
databricks bundle run windmill_pipeline --profile e2-demo-field-eng
```

### Development Servers
```bash
# Frontend (port 5173)
cd client && npm run dev

# Backend (port 8001)
uvicorn server.app:app --host 0.0.0.0 --port 8001 --reload
```

### Bundle Management
```bash
# Destroy bundle
cd pipeline && databricks bundle destroy --profile e2-demo-field-eng --force-lock
```

## Key Files Modified

### Frontend
- `src/pages/Dashboard.tsx` - Main dashboard with windmill monitoring
- `src/components/Layout.tsx` - Simplified single-page navigation
- `src/App.tsx` - Removed routing, direct Dashboard rendering
- `src/main.tsx` - Removed BrowserRouter
- `package.json` - Optimized dependencies (removed react-router-dom, tailwind, eslint)

### Backend
- `server/routers/pipeline_analysis.py` - Timeout handling, empty array fallbacks
- `server/services/pipeline_service.py` - Enhanced error handling, sync SDK calls

### Pipeline
- `src/windmill_data_generator/main.py` - Generates `raw_sensor_data` with realistic windmill metrics
- `src/lakeflow_pipeline.py` - DLT bronze/silver/gold processing with event logging

## Completed Optimizations

1. **Node Modules Cleanup**: 10,000+ � 93 packages (99% reduction)
2. **UI Simplification**: Single Dashboard page, removed Pipeline Analysis/Table Explorer tabs
3. **Performance**: Removed slow Tables/Data Assets sections
4. **Visual**: Windmill icon, removed promotional text
5. **Architecture**: Single-page app (no routing overhead)
6. **Error Handling**: Graceful API timeouts and fallbacks
7. **Pipeline Search Optimization**: Implemented efficient name LIKE filtering for Databricks SDK calls

## Technical Specifications

- **React**: 18.2.0 with TypeScript
- **Build Tool**: Vite
- **State Management**: TanStack Query
- **Icons**: Lucide React (windmill icon)
- **Styling**: Inline styles with gradient themes
- **Backend**: FastAPI with Databricks SDK
- **Data Processing**: Delta Live Tables with Unity Catalog

## Current Status
 **COMPLETED** - All requirements implemented and optimized
 Real-time windmill IoT monitoring dashboard
 Databricks integration with event log publishing
 Performance optimized with minimal dependencies
 Single-page application with windmill branding

## API Performance Solutions

### Pipeline Search Optimization
- **Problem**: Original `list_pipelines()` API returned pipelines in arbitrary order, requiring scan of 200+ results to find windmill pipelines
- **Solution**: Implemented `filter="name LIKE '%windmill%'"` parameter for efficient server-side filtering
- **Fallback**: Graceful degradation to manual search if filtering is not supported
- **Performance**: Reduced API call time from 60+ seconds to under 5 seconds

### Implementation Details
```python
# Primary approach: Use name LIKE filter
pipeline_list = client.pipelines.list_pipelines(filter="name LIKE '%windmill%'")

# Fallback: Manual search with limits if filtering fails
for pipeline in client.pipelines.list_pipelines():
    if "windmill" in pipeline.name.lower():
        # Process pipeline
```

## Troubleshooting Notes

- **Unity Catalog Issues**: Use `data_security_mode: "SINGLE_USER"` and `runtime_engine: "PHOTON"`
- **API Timeouts**: Backend returns empty arrays instead of 500 errors
- **Schema Resolution**: Environment variables must match databricks.yml catalog/target
- **Bundle Deployment**: Use `--force-lock` flag if deployment hangs
- **Pipeline Search**: Use LIKE filtering for efficient searches; fallback to limited manual search