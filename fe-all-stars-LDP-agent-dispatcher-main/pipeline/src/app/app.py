#!/usr/bin/env python3

from flask import Flask, render_template_string, jsonify
from flask_cors import CORS
import os
import time
import asyncio
import concurrent.futures
from io import StringIO

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Capture print statements for debugging
log_output = StringIO()

# Simple caching for API responses
_cache = {}
CACHE_DURATION = 30  # Cache for 30 seconds

def get_cached_or_fetch(key, fetch_function):
    """Simple cache that stores results for CACHE_DURATION seconds"""
    now = time.time()
    if key in _cache and now - _cache[key]['timestamp'] < CACHE_DURATION:
        return _cache[key]['data']

    # Fetch fresh data
    data = fetch_function()
    _cache[key] = {'data': data, 'timestamp': now}
    return data

# HTML template for the FE All Stars Agent Dispatcher dashboard
DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FE All Stars Agent Dispatcher</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #1e1b4b 0%, #7c3aed 50%, #ec4899 100%);
            min-height: 100vh;
            color: white;
        }

        .container {
            display: flex;
            min-height: 100vh;
        }

        .sidebar {
            width: 250px;
            background: rgba(30, 27, 75, 0.8);
            backdrop-filter: blur(10px);
            padding: 20px;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 40px;
        }

        .logo-icon {
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, #06b6d4, #3b82f6);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .nav-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 16px;
            border-radius: 8px;
            background: rgba(59, 130, 246, 0.2);
            color: white;
            text-decoration: none;
            margin-bottom: 8px;
        }

        .main-content {
            flex: 1;
            padding: 40px;
        }

        .header {
            text-align: center;
            margin-bottom: 40px;
        }

        .header h1 {
            font-size: 3rem;
            font-weight: bold;
            background: linear-gradient(135deg, #06b6d4, #3b82f6, #8b5cf6);
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 10px;
        }

        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }

        .metric-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 24px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .metric-icon {
            width: 48px;
            height: 48px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 16px;
            font-size: 24px;
        }

        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            margin-bottom: 8px;
        }

        .metric-label {
            font-size: 0.875rem;
            opacity: 0.8;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .pipeline-section {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .section-header {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 20px;
        }

        .section-icon {
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, #06b6d4, #3b82f6);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
        }

        .pipeline-item {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .pipeline-info {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .pipeline-icon {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, #06b6d4, #3b82f6);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
        }

        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .status-running {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
        }

        .blue { background: linear-gradient(135deg, #06b6d4, #0891b2); }
        .green { background: linear-gradient(135deg, #10b981, #059669); }
        .orange { background: linear-gradient(135deg, #f59e0b, #d97706); }
        .purple { background: linear-gradient(135deg, #8b5cf6, #7c3aed); }
    </style>
</head>
<body>
    <div class="container">
        <div class="sidebar">
            <div class="logo">
                <div class="logo-icon">🏠</div>
                <span style="font-weight: 600;">Dashboard</span>
            </div>
            <a href="#" class="nav-item">
                <span>📊</span>
                <span>Overview & metrics</span>
            </a>
            <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid rgba(255,255,255,0.1);">
                <a href="#" class="nav-item" style="background: rgba(255,255,255,0.1);">
                    <span>⚙️</span>
                    <span>Settings</span>
                </a>
            </div>
        </div>

        <div class="main-content">
            <div class="header">
                <h1>FE All Stars Agent Dispatcher</h1>
            </div>

            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-icon blue">🔗</div>
                    <div class="metric-value" id="total-count">Loading...</div>
                    <div class="metric-label">Jobs & Pipelines</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon green">⚡</div>
                    <div class="metric-value" id="active-count">Loading...</div>
                    <div class="metric-label">Active</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon orange">✓</div>
                    <div class="metric-value" id="idle-count">Loading...</div>
                    <div class="metric-label">Idle</div>
                </div>
            </div>

            <div class="pipeline-section">
                <div class="section-header">
                    <div class="section-icon">🔗</div>
                    <div>
                        <h2 style="font-size: 1.25rem; font-weight: 600;">Pipeline Overview</h2>
                        <p style="opacity: 0.7; font-size: 0.875rem;">Real-time pipeline intelligence</p>
                    </div>
                </div>

                <div id="pipeline-list">
                    <div style="text-align: center; padding: 20px;">Loading pipeline data...</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Load live data from APIs
        async function loadDashboardData() {
            try {
                // Load status metrics
                const statusResponse = await fetch('/api/status');
                const statusData = await statusResponse.json();

                document.getElementById('total-count').textContent = statusData.total_pipelines_jobs || 0;
                document.getElementById('active-count').textContent = statusData.active || 0;
                document.getElementById('idle-count').textContent = statusData.idle || 0;

                // Load pipeline data
                const pipelineResponse = await fetch('/api/pipelines');
                const pipelineData = await pipelineResponse.json();

                const pipelineList = document.getElementById('pipeline-list');
                if (pipelineData.pipelines && pipelineData.pipelines.length > 0) {
                    pipelineList.innerHTML = pipelineData.pipelines.map(pipeline => `
                        <div class="pipeline-item">
                            <div class="pipeline-info">
                                <div class="pipeline-icon">🔗</div>
                                <div>
                                    <div style="font-weight: 600; margin-bottom: 4px;">
                                        ${pipeline.name}
                                    </div>
                                    <div style="font-size: 0.875rem; opacity: 0.7;">
                                        ${pipeline.catalog}.${pipeline.target}
                                    </div>
                                </div>
                            </div>
                            <div class="status-badge ${pipeline.state.toUpperCase().includes('RUNNING') ? 'status-running' : ''}"
                                 style="background: ${pipeline.state.toUpperCase().includes('RUNNING') ? 'linear-gradient(135deg, #10b981, #059669)' : 'linear-gradient(135deg, #6b7280, #4b5563)'}">
                                ${pipeline.state}
                            </div>
                        </div>
                    `).join('');
                } else {
                    pipelineList.innerHTML = '<div style="text-align: center; padding: 20px;">No windmill pipelines found</div>';
                }

                // Load job data and add to pipeline list
                const jobResponse = await fetch('/api/jobs');
                const jobData = await jobResponse.json();

                if (jobData.jobs && jobData.jobs.length > 0) {
                    const jobItems = jobData.jobs.map(job => `
                        <div class="pipeline-item">
                            <div class="pipeline-info">
                                <div class="pipeline-icon">⚙️</div>
                                <div>
                                    <div style="font-weight: 600; margin-bottom: 4px;">
                                        ${job.name}
                                    </div>
                                    <div style="font-size: 0.875rem; opacity: 0.7;">
                                        Last run: ${job.last_run}
                                    </div>
                                </div>
                            </div>
                            <div class="status-badge ${job.state === 'RUNNING' ? 'status-running' : ''}"
                                 style="background: ${job.state === 'RUNNING' ? 'linear-gradient(135deg, #10b981, #059669)' : 'linear-gradient(135deg, #6b7280, #4b5563)'}">
                                ${job.state}
                            </div>
                        </div>
                    `).join('');

                    pipelineList.innerHTML += jobItems;
                }

            } catch (error) {
                console.error('Error loading dashboard data:', error);
                document.getElementById('total-count').textContent = 'Error';
                document.getElementById('active-count').textContent = 'Error';
                document.getElementById('idle-count').textContent = 'Error';
                document.getElementById('pipeline-list').innerHTML = '<div style="text-align: center; padding: 20px; color: #ef4444;">Error loading data</div>';
            }
        }

        // Load data on page load
        document.addEventListener('DOMContentLoaded', loadDashboardData);

        // Refresh data every 30 seconds
        // Refresh data every 60 seconds instead of 30 to reduce load
        setInterval(loadDashboardData, 60000);
    </script>
</body>
</html>
'''

# Initialize Databricks client with proper authentication for Apps
def get_databricks_client():
    try:
        from databricks.sdk import WorkspaceClient

        host = os.environ.get('DATABRICKS_HOST')
        token = os.environ.get('DATABRICKS_TOKEN')

        log_print(f"DATABRICKS_HOST: {host}")
        log_print(f"DATABRICKS_TOKEN present: {'Yes' if token else 'No'}")

        if not host or not token:
            log_print("Missing DATABRICKS_HOST or DATABRICKS_TOKEN environment variables")
            # Try default WorkspaceClient() which might use other auth methods
            return WorkspaceClient()

        return WorkspaceClient(host=host, token=token)
    except Exception as e:
        log_print(f"Error initializing Databricks client: {e}")
        return None

# Alternative approach: Direct REST API call with search
def search_pipelines_by_name(search_term="windmill"):
    try:
        import requests
        host = os.environ.get('DATABRICKS_HOST', 'e2-demo-field-eng.cloud.databricks.com')

        # Try to get auth token from environment or use default auth
        headers = {'Authorization': 'Bearer YOUR_TOKEN_HERE'}  # This won't work but let's see

        # Direct API call to search pipelines
        url = f"https://{host}/api/2.0/pipelines"
        response = requests.get(url, headers=headers, timeout=10)

        log_print(f"Direct API response status: {response.status_code}")
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        log_print(f"Direct API call failed: {e}")
        return None

# Custom print function that captures output
def log_print(message):
    print(message)
    log_output.write(f"{message}\n")

# Get current workspace user for filtering
def get_current_user():
    try:
        client = get_databricks_client()
        if client:
            current_user = client.current_user.me()
            user_short_name = current_user.user_name.split('@')[0] if current_user.user_name else None
            log_print(f"Current user detected: {user_short_name}")
            return user_short_name
    except Exception as e:
        log_print(f"Error getting current user: {e}")
    return None

@app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_TEMPLATE)

@app.route('/api/pipelines')
def get_pipelines():
    """Get live pipeline data with caching"""
    def fetch_pipelines():
        start_time = time.time()
        try:
            client = get_databricks_client()
            if not client:
                return {"pipelines": [], "error": "Could not connect to Databricks"}

            pipelines = []

            # Try efficient name LIKE filter first, fallback to limited search
            try:
                log_print(f"Starting pipeline fetch at {start_time}")

                # Primary approach: Try using filter parameter
                try:
                    log_print("Attempting name LIKE filter for windmill pipelines...")
                    pipeline_list = client.pipelines.list_pipelines(filter="name LIKE '%windmill%'")
                    log_print("Filter parameter accepted, processing results...")

                    for pipeline in pipeline_list:
                        # Get state and convert to string for JSON serialization
                        state_obj = getattr(pipeline, 'state', 'UNKNOWN')
                        state = str(state_obj) if state_obj else 'UNKNOWN'

                        pipelines.append({
                            "id": getattr(pipeline, 'pipeline_id', 'unknown'),
                            "name": pipeline.name,
                            "state": state,
                            "catalog": "users",
                            "target": "unknown",
                            "last_modified": ""
                        })
                        log_print(f"Found windmill pipeline: {pipeline.name} - State: {state}")

                    log_print(f"Filter approach found {len(pipelines)} windmill pipelines")

                except TypeError as filter_error:
                    log_print(f"Filter parameter not supported: {filter_error}")
                    # Fallback: Try searching more pipelines since 200 wasn't enough
                    log_print("Falling back to expanded manual search...")
                    pipeline_count = 0
                    max_pipelines_to_check = 500  # Increase limit significantly

                    for pipeline in client.pipelines.list_pipelines():
                        pipeline_count += 1
                        if pipeline.name and "windmill" in pipeline.name.lower():
                            # Get state and convert to string for JSON serialization
                            state_obj = getattr(pipeline, 'state', 'UNKNOWN')
                            state = str(state_obj) if state_obj else 'UNKNOWN'

                            pipelines.append({
                                "id": getattr(pipeline, 'pipeline_id', 'unknown'),
                                "name": pipeline.name,
                                "state": state,
                                "catalog": "users",
                                "target": "unknown",
                                "last_modified": ""
                            })
                            log_print(f"Found windmill pipeline: {pipeline.name} - State: {state}")

                        if pipeline_count >= max_pipelines_to_check:
                            log_print(f"Reached expanded limit of {max_pipelines_to_check} pipelines")
                            break

                    log_print(f"Manual search checked {pipeline_count} pipelines, found {len(pipelines)} windmill pipelines")
            except Exception as e:
                error_msg = f"Error listing pipelines: {str(e)}"
                log_print(error_msg)
                return {"pipelines": [], "error": error_msg}

            end_time = time.time()
            log_print(f"Pipeline fetch completed in {end_time - start_time:.2f} seconds")
            return {"pipelines": pipelines}

        except Exception as e:
            return {"pipelines": [], "error": f"Error fetching pipelines: {str(e)}"}

    # Use caching to avoid repeated expensive API calls
    try:
        result = get_cached_or_fetch("pipelines", fetch_pipelines)
        # Ensure clean JSON response
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        return response
    except Exception as e:
        log_print(f"Error in get_pipelines endpoint: {e}")
        error_response = jsonify({"pipelines": [], "error": f"Endpoint error: {str(e)}"})
        error_response.headers.add('Access-Control-Allow-Origin', '*')
        return error_response

@app.route('/api/jobs')
def get_jobs():
    """Get live job data - simplified to avoid timeouts"""
    try:
        # Return empty jobs for now since jobs API causes timeouts
        return jsonify({"jobs": []})
    except Exception as e:
        return jsonify({"jobs": [], "error": f"Error fetching jobs: {str(e)}"})

@app.route('/api/status')
def get_status():
    """Get combined status for dashboard metrics with caching"""
    def fetch_status():
        try:
            client = get_databricks_client()
            if not client:
                return {
                    "total_pipelines_jobs": 0,
                    "active": 0,
                    "idle": 0,
                    "error": "Could not connect to Databricks"
                }

            total_count = 0
            active_count = 0
            idle_count = 0

            # Try efficient filtering first, then expanded search
            try:
                # Try filter approach first
                try:
                    log_print("Status endpoint: Attempting name LIKE filter...")
                    pipeline_list = client.pipelines.list_pipelines(filter="name LIKE '%windmill%'")

                    for pipeline in pipeline_list:
                        if pipeline.name and "windmill" in pipeline.name.lower():
                            total_count += 1
                            state_obj = getattr(pipeline, 'state', 'UNKNOWN')
                            state = str(state_obj) if state_obj else 'UNKNOWN'
                            if "RUNNING" in state.upper():
                                active_count += 1
                            else:
                                idle_count += 1

                    log_print(f"Status endpoint: Filter found {total_count} windmill pipelines")

                except TypeError as filter_error:
                    log_print(f"Status endpoint: Filter not supported, using expanded search...")
                    pipeline_count = 0
                    max_pipelines_to_check = 500  # Expanded limit

                    for pipeline in client.pipelines.list_pipelines():
                        pipeline_count += 1
                        if pipeline_count > max_pipelines_to_check:
                            break

                        if pipeline.name and "windmill" in pipeline.name.lower():
                            total_count += 1
                            state_obj = getattr(pipeline, 'state', 'UNKNOWN')
                            state = str(state_obj) if state_obj else 'UNKNOWN'
                            if "RUNNING" in state.upper():
                                active_count += 1
                            else:
                                idle_count += 1
            except Exception as e:
                log_print(f"Error counting pipelines: {e}")

            # Skip jobs for now since they cause timeouts
            # Will only show pipeline metrics

            return {
                "total_pipelines_jobs": total_count,
                "active": active_count,
                "idle": idle_count
            }

        except Exception as e:
            return {
                "total_pipelines_jobs": 0,
                "active": 0,
                "idle": 0,
                "error": f"Error fetching status: {str(e)}"
            }

    # Use caching to improve performance
    try:
        result = get_cached_or_fetch("status", fetch_status)
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        return response
    except Exception as e:
        log_print(f"Error in get_status endpoint: {e}")
        error_response = jsonify({"total_pipelines_jobs": 0, "active": 0, "idle": 0, "error": f"Status endpoint error: {str(e)}"})
        error_response.headers.add('Access-Control-Allow-Origin', '*')
        return error_response

@app.route('/api/logs')
def get_logs():
    """Get application logs for debugging"""
    logs = log_output.getvalue()
    return jsonify({"logs": logs.split('\n')})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)