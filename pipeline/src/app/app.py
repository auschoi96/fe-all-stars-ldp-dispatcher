#!/usr/bin/env python3

from flask import Flask, render_template_string, jsonify
from flask_cors import CORS
import os
import time
import asyncio
import concurrent.futures
from io import StringIO
import psycopg2

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

        .pipeline-section, .tickets-section {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            margin-bottom: 40px;
        }

        /* Tickets Table Styles */
        .tickets-table-container {
            overflow: auto;
            border-radius: 12px;
            background: rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.1);
            max-height: 400px;
        }

        .tickets-table {
            width: 100%;
            border-collapse: collapse;
            color: white;
        }

        .tickets-table thead {
            background: rgba(255, 255, 255, 0.15);
            position: sticky;
            top: 0;
            z-index: 10;
        }

        .tickets-table th {
            padding: 16px 20px;
            text-align: left;
            font-weight: 600;
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            background: rgba(255, 255, 255, 0.15);
        }

        .tickets-table td {
            padding: 16px 20px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            vertical-align: top;
        }

        .tickets-table tbody tr:hover {
            background: rgba(255, 255, 255, 0.05);
        }

        .loading-cell {
            text-align: center;
            color: rgba(255, 255, 255, 0.7);
            font-style: italic;
            padding: 40px 20px;
        }

        /* Collapsible Details Styles */
        .ticket-details-cell {
            position: relative;
        }

        .details-preview {
            max-height: 60px;
            overflow: hidden;
            line-height: 1.5;
            position: relative;
        }

        .details-preview.collapsed::after {
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 20px;
            background: linear-gradient(transparent, rgba(0, 0, 0, 0.3));
        }

        .details-full {
            display: none;
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        .details-toggle {
            background: linear-gradient(135deg, #06b6d4, #0891b2);
            color: white;
            border: none;
            padding: 4px 12px;
            border-radius: 6px;
            font-size: 0.75rem;
            cursor: pointer;
            margin-top: 8px;
            transition: all 0.3s ease;
        }

        .details-toggle:hover {
            background: linear-gradient(135deg, #0891b2, #0e7490);
            transform: translateY(-1px);
        }

        .turbine-id-cell {
            font-weight: 600;
            color: #06b6d4;
            min-width: 120px;
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

        /* Modal Styles */
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            backdrop-filter: blur(5px);
        }

        .modal-content {
            background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);
            margin: 5% auto;
            padding: 0;
            border-radius: 16px;
            width: 90%;
            max-width: 800px;
            max-height: 80vh;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
        }

        .modal-header {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px 30px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .modal-header h2 {
            margin: 0;
            color: white;
            font-size: 1.5rem;
            font-weight: 600;
        }

        .close {
            color: rgba(255, 255, 255, 0.7);
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
            transition: color 0.3s ease;
        }

        .close:hover {
            color: white;
        }

        .modal-body {
            padding: 20px 30px 30px;
            max-height: 60vh;
            overflow-y: auto;
        }

        .loading {
            text-align: center;
            color: rgba(255, 255, 255, 0.7);
            padding: 40px;
            font-style: italic;
        }

        .ticket-item {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            margin-bottom: 16px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            overflow: hidden;
            transition: all 0.3s ease;
        }

        .ticket-item:hover {
            background: rgba(255, 255, 255, 0.08);
            border-color: rgba(255, 255, 255, 0.2);
        }

        .ticket-header {
            padding: 16px 20px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            user-select: none;
        }

        .ticket-header:hover {
            background: rgba(255, 255, 255, 0.05);
        }

        .ticket-title {
            font-weight: 600;
            color: white;
            font-size: 1.1rem;
        }

        .ticket-toggle {
            color: rgba(255, 255, 255, 0.7);
            font-size: 1.2rem;
            transition: transform 0.3s ease;
        }

        .ticket-toggle.expanded {
            transform: rotate(180deg);
        }

        .ticket-details {
            padding: 0 20px 20px;
            display: none;
            border-top: 1px solid rgba(255, 255, 255, 0.1);
            background: rgba(0, 0, 0, 0.1);
        }

        .ticket-details.expanded {
            display: block;
            animation: slideDown 0.3s ease;
        }

        @keyframes slideDown {
            from {
                opacity: 0;
                max-height: 0;
            }
            to {
                opacity: 1;
                max-height: 500px;
            }
        }

        .ticket-content {
            color: rgba(255, 255, 255, 0.9);
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
            margin-top: 16px;
        }

        /* Clickable metric card style */
        .metric-card.clickable {
            cursor: pointer;
            transition: all 0.3s ease;
            position: relative;
        }

        .metric-card.clickable:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
            background: rgba(255, 255, 255, 0.15);
        }

        .click-hint {
            font-size: 0.7rem;
            color: rgba(255, 255, 255, 0.6);
            margin-top: 4px;
            opacity: 0;
            transition: opacity 0.3s ease;
        }

        .metric-card.clickable:hover .click-hint {
            opacity: 1;
        }

        /* Enhanced modal styles for better visibility */
        .modal {
            display: none;
            position: fixed;
            z-index: 10000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.8);
            backdrop-filter: blur(8px);
            animation: fadeIn 0.3s ease;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        .modal-content {
            background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);
            margin: 3% auto;
            padding: 0;
            border-radius: 16px;
            width: 95%;
            max-width: 900px;
            max-height: 85vh;
            overflow: hidden;
            border: 2px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
            animation: slideIn 0.3s ease;
        }

        @keyframes slideIn {
            from {
                transform: translateY(-50px);
                opacity: 0;
            }
            to {
                transform: translateY(0);
                opacity: 1;
            }
        }
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

                <div class="metric-card">
                    <div class="metric-icon purple">🎫</div>
                    <div class="metric-value" id="open-tickets-count">Loading...</div>
                    <div class="metric-label">Open Tickets</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon blue">📋</div>
                    <div class="metric-value" id="need-to-file-count">Loading...</div>
                    <div class="metric-label">Need to File</div>
                </div>
            </div>

            <div class="tickets-section">
                <div class="section-header">
                    <div class="section-icon">🎫</div>
                    <div>
                        <h2 style="font-size: 1.25rem; font-weight: 600;">Open Tickets</h2>
                        <p style="opacity: 0.7; font-size: 0.875rem;">Turbine maintenance tickets requiring attention</p>
                    </div>
                </div>

                <div class="tickets-table-container">
                    <table class="tickets-table">
                        <thead>
                            <tr>
                                <th>Turbine ID</th>
                                <th>Details</th>
                            </tr>
                        </thead>
                        <tbody id="tickets-table-body">
                            <tr>
                                <td colspan="2" class="loading-cell">Loading ticket data...</td>
                            </tr>
                        </tbody>
                    </table>
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

        // Load tickets data from Postgres
        async function loadTickets() {
            try {
                const ticketsResponse = await fetch('/api/tickets');
                const ticketsData = await ticketsResponse.json();

                document.getElementById('open-tickets-count').textContent = ticketsData.open_tickets || 0;
                document.getElementById('need-to-file-count').textContent = ticketsData.need_to_file_tickets || 0;

                // Load ticket details for the table
                loadTicketsTable();
            } catch (error) {
                console.error('Error loading tickets:', error);
                document.getElementById('open-tickets-count').textContent = 'Error';
                document.getElementById('need-to-file-count').textContent = 'Error';
            }
        }

        // Load ticket details for the table
        async function loadTicketsTable() {
            const tableBody = document.getElementById('tickets-table-body');

            try {
                const response = await fetch('/api/ticket-details');
                const data = await response.json();

                if (data.tickets && data.tickets.length > 0) {
                    tableBody.innerHTML = data.tickets.map((ticket, index) => {
                        const shortDetails = ticket.details.substring(0, 150);
                        const hasMore = ticket.details.length > 150;

                        return `
                            <tr>
                                <td class="turbine-id-cell">${ticket.turbine_id}</td>
                                <td class="ticket-details-cell">
                                    <div class="details-preview ${hasMore ? 'collapsed' : ''}" id="preview-${index}">
                                        ${shortDetails}${hasMore ? '...' : ''}
                                    </div>
                                    <div class="details-full" id="full-${index}">
                                        ${ticket.details}
                                    </div>
                                    ${hasMore ? `<button class="details-toggle" onclick="toggleTicketDetails(${index})">Show More</button>` : ''}
                                </td>
                            </tr>
                        `;
                    }).join('');
                } else {
                    tableBody.innerHTML = '<tr><td colspan="2" class="loading-cell">No open tickets found</td></tr>';
                }
            } catch (error) {
                console.error('Error loading ticket details:', error);
                tableBody.innerHTML = '<tr><td colspan="2" class="loading-cell" style="color: #ef4444;">Error loading ticket details</td></tr>';
            }
        }

        // Toggle ticket details function
        function toggleTicketDetails(index) {
            const preview = document.getElementById(`preview-${index}`);
            const full = document.getElementById(`full-${index}`);
            const button = event.target;

            if (full.style.display === 'none' || full.style.display === '') {
                preview.style.display = 'none';
                full.style.display = 'block';
                button.textContent = 'Show Less';
            } else {
                preview.style.display = 'block';
                full.style.display = 'none';
                button.textContent = 'Show More';
            }
        }

        // Make toggleTicketDetails available globally
        window.toggleTicketDetails = toggleTicketDetails;

        // Load data on page load
        document.addEventListener('DOMContentLoaded', function() {
            loadDashboardData();
            loadTickets(); // Load tickets immediately
        });

        // Refresh dashboard data every 60 seconds
        setInterval(loadDashboardData, 60000);

        // Refresh tickets every 15 seconds for more frequent updates
        setInterval(loadTickets, 15000);
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

# M2M authentication for Postgres connection
def get_postgres_connection():
    """Connect to Databricks Postgres using M2M authentication with secrets"""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config

        log_print("=== POSTGRES M2M CONNECTION ATTEMPT ===")

        # Retrieve secrets from Databricks secret scope using app runtime auth
        try:
            # Use app runtime authentication to get secrets
            app_client = WorkspaceClient()
            client_id_response = app_client.secrets.get_secret(scope="lakebase-auth", key="client_id")
            client_secret_response = app_client.secrets.get_secret(scope="lakebase-auth", key="client_secret")

            # Decode base64 encoded secrets and strip any whitespace
            import base64
            client_id = base64.b64decode(client_id_response.value).decode('utf-8').strip()
            client_secret = base64.b64decode(client_secret_response.value).decode('utf-8').strip()
            log_print("SUCCESS: Retrieved and decoded credentials from Databricks secrets")
        except Exception as e:
            log_print(f"ERROR: Failed to retrieve secrets from Databricks: {e}")
            return None

        # Create a separate Config object specifically for service principal auth
        log_print(f"M2M Config - Client ID: {client_id[:8]}...")  # Only show first 8 chars for security
        log_print(f"M2M Config - Host: https://e2-demo-field-eng.cloud.databricks.com")

        try:
            # Create config for service principal authentication
            cfg = Config(
                host='https://e2-demo-field-eng.cloud.databricks.com',
                client_id=client_id,
                client_secret=client_secret
            )
            log_print("Created Databricks Config object with service principal")

            auth_headers = cfg.authenticate()
            log_print(f"Auth headers keys: {list(auth_headers.keys())}")

            if 'Authorization' not in auth_headers:
                log_print("ERROR: No Authorization header in M2M response")
                return None

            oauth_token = auth_headers['Authorization'].replace('Bearer ', '')
            log_print(f"SUCCESS: Got OAuth token for Postgres (length: {len(oauth_token)})")
            log_print(f"Token starts with: {oauth_token[:20]}...")
        except Exception as auth_error:
            log_print(f"ERROR: Service principal authentication failed: {auth_error}")
            return None

        # Connection parameters
        pg_host = 'instance-33607cf0-86c1-4306-a5c3-2464761fa328.database.cloud.databricks.com'
        pg_database = 'databricks_postgres'
        pg_port = '5432'

        log_print(f"Attempting connection to: {pg_host}:{pg_port}/{pg_database}")

        # Try multiple authentication approaches for Databricks PostgreSQL with OAuth
        connection_attempts = [
            {'user': oauth_token, 'description': 'username=oauth_token'},
            {'user': 'token', 'description': 'username=token'},
            {'user': client_id, 'description': 'username=service_principal_id'},
            {'user': '', 'description': 'empty username (defaults to app)'},
            {'user': 'databricks', 'description': 'username=databricks'}
        ]

        for i, attempt in enumerate(connection_attempts, 1):
            try:
                log_print(f"Attempt {i}: Trying {attempt['description']}")
                conn = psycopg2.connect(
                    host=pg_host,
                    database=pg_database,
                    user=attempt['user'],
                    password=oauth_token,
                    port=pg_port,
                    sslmode='require',
                    connect_timeout=10
                )
                log_print(f"SUCCESS: Connected with {attempt['description']}!")
                return conn

            except psycopg2.Error as pg_error:
                log_print(f"Attempt {i} failed - PostgreSQL Error: {pg_error}")
                log_print(f"Error code: {pg_error.pgcode if hasattr(pg_error, 'pgcode') else 'N/A'}")
            except Exception as conn_error:
                log_print(f"Attempt {i} failed - Connection Error: {conn_error}")

        log_print("ERROR: All connection attempts failed")
        return None

    except Exception as e:
        log_print(f"FATAL ERROR in get_postgres_connection: {e}")
        log_print(f"Error type: {type(e).__name__}")
        import traceback
        log_print(f"Traceback: {traceback.format_exc()}")
        return None

def get_open_tickets_count():
    """Get count of open tickets from turbine_monitoring table"""
    log_print("=== GETTING OPEN TICKETS COUNT ===")
    try:
        conn = get_postgres_connection()
        if not conn:
            log_print("ERROR: Could not establish Postgres connection")
            return 0

        log_print("SUCCESS: Got Postgres connection, executing query...")

        with conn.cursor() as cursor:
            # First, let's check if the table exists and what data is in it
            try:
                log_print("Checking if turbine_monitoring table exists...")
                cursor.execute("SELECT to_regclass('public.turbine_monitoring')")
                table_exists = cursor.fetchone()[0]
                log_print(f"Table exists check result: {table_exists}")

                if table_exists:
                    # Check total row count
                    cursor.execute("SELECT COUNT(*) FROM turbine_monitoring")
                    total_count = cursor.fetchone()[0]
                    log_print(f"Total rows in turbine_monitoring: {total_count}")

                    # Check distinct ticket_status values
                    cursor.execute("SELECT DISTINCT ticket_status, COUNT(*) FROM turbine_monitoring GROUP BY ticket_status")
                    status_counts = cursor.fetchall()
                    log_print(f"Ticket status breakdown: {status_counts}")

                    # Execute the main query
                    log_print("Executing main query: SELECT COUNT(*) FROM turbine_monitoring WHERE ticket_status = 'Open'")
                    cursor.execute("SELECT COUNT(*) FROM turbine_monitoring WHERE ticket_status = 'Open'")
                    count = cursor.fetchone()[0]
                    log_print(f"SUCCESS: Found {count} open tickets")
                    return count
                else:
                    log_print("ERROR: turbine_monitoring table does not exist")
                    return 0

            except psycopg2.Error as query_error:
                log_print(f"PostgreSQL Query Error: {query_error}")
                log_print(f"Query Error code: {query_error.pgcode if hasattr(query_error, 'pgcode') else 'N/A'}")
                return 0

    except Exception as e:
        log_print(f"ERROR in get_open_tickets_count: {e}")
        log_print(f"Error type: {type(e).__name__}")
        import traceback
        log_print(f"Traceback: {traceback.format_exc()}")
        return 0
    finally:
        if 'conn' in locals() and conn:
            try:
                conn.close()
                log_print("Postgres connection closed")
            except:
                pass

def get_need_to_file_tickets_count():
    """Get count of 'Need to File' tickets from turbine_monitoring table"""
    log_print("=== GETTING NEED TO FILE TICKETS COUNT ===")
    try:
        conn = get_postgres_connection()
        if not conn:
            log_print("ERROR: Could not establish Postgres connection")
            return 0

        log_print("SUCCESS: Got Postgres connection, executing query...")

        with conn.cursor() as cursor:
            try:
                log_print("Checking if turbine_monitoring table exists...")
                cursor.execute("SELECT to_regclass('public.turbine_monitoring')")
                table_exists = cursor.fetchone()[0]
                log_print(f"Table exists check result: {table_exists}")

                if table_exists:
                    # Check total row count
                    cursor.execute("SELECT COUNT(*) FROM turbine_monitoring")
                    total_count = cursor.fetchone()[0]
                    log_print(f"Total rows in turbine_monitoring: {total_count}")

                    # Check distinct ticket_status values
                    cursor.execute("SELECT DISTINCT ticket_status, COUNT(*) FROM turbine_monitoring GROUP BY ticket_status")
                    status_counts = cursor.fetchall()
                    log_print(f"Ticket status breakdown: {status_counts}")

                    # Execute the main query
                    log_print("Executing main query: SELECT COUNT(*) FROM turbine_monitoring WHERE ticket_status = 'Need to File'")
                    cursor.execute("SELECT COUNT(*) FROM turbine_monitoring WHERE ticket_status = 'Need to File'")
                    count = cursor.fetchone()[0]
                    log_print(f"SUCCESS: Found {count} 'Need to File' tickets")
                    return count
                else:
                    log_print("ERROR: turbine_monitoring table does not exist")
                    return 0

            except psycopg2.Error as query_error:
                log_print(f"PostgreSQL Query Error: {query_error}")
                log_print(f"Query Error code: {query_error.pgcode if hasattr(query_error, 'pgcode') else 'N/A'}")
                return 0

    except Exception as e:
        log_print(f"ERROR in get_need_to_file_tickets_count: {e}")
        log_print(f"Error type: {type(e).__name__}")
        import traceback
        log_print(f"Traceback: {traceback.format_exc()}")
        return 0
    finally:
        if 'conn' in locals() and conn:
            try:
                conn.close()
                log_print("Postgres connection closed")
            except:
                pass

def get_open_ticket_details():
    """Get detailed information for open tickets from turbine_monitoring table"""
    log_print("=== GETTING OPEN TICKET DETAILS ===")
    try:
        conn = get_postgres_connection()
        if not conn:
            log_print("ERROR: Could not establish Postgres connection")
            return []

        log_print("SUCCESS: Got Postgres connection, executing query...")

        with conn.cursor() as cursor:
            try:
                log_print("Checking if turbine_monitoring table exists...")
                cursor.execute("SELECT to_regclass('public.turbine_monitoring')")
                table_exists = cursor.fetchone()[0]
                log_print(f"Table exists check result: {table_exists}")

                if table_exists:
                    # Execute the main query to get ticket details
                    log_print("Executing query: SELECT turbine_id, turbine_details_and_status FROM turbine_monitoring WHERE ticket_status = 'Open'")
                    cursor.execute("SELECT turbine_id, turbine_details_and_status FROM turbine_monitoring WHERE ticket_status = 'Open'")
                    results = cursor.fetchall()

                    # Convert to list of dictionaries
                    tickets = []
                    for row in results:
                        tickets.append({
                            "turbine_id": row[0],
                            "details": row[1] if row[1] else "No details available"
                        })

                    log_print(f"SUCCESS: Found {len(tickets)} open ticket details")
                    return tickets
                else:
                    log_print("ERROR: turbine_monitoring table does not exist")
                    return []

            except psycopg2.Error as query_error:
                log_print(f"PostgreSQL Query Error: {query_error}")
                log_print(f"Query Error code: {query_error.pgcode if hasattr(query_error, 'pgcode') else 'N/A'}")
                return []

    except Exception as e:
        log_print(f"ERROR in get_open_ticket_details: {e}")
        log_print(f"Error type: {type(e).__name__}")
        import traceback
        log_print(f"Traceback: {traceback.format_exc()}")
        return []
    finally:
        if 'conn' in locals() and conn:
            try:
                conn.close()
                log_print("Postgres connection closed")
            except:
                pass

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

@app.route('/api/tickets')
def get_tickets():
    """Get open tickets and need to file tickets counts from Postgres"""
    def fetch_tickets_counts():
        open_count = get_open_tickets_count()
        need_to_file_count = get_need_to_file_tickets_count()
        return {"open_tickets": open_count, "need_to_file_tickets": need_to_file_count}

    try:
        counts = get_cached_or_fetch("tickets_counts", fetch_tickets_counts)
        return jsonify(counts)
    except Exception as e:
        return jsonify({"open_tickets": 0, "need_to_file_tickets": 0, "error": f"Error fetching tickets: {str(e)}"})

@app.route('/api/ticket-details')
def get_ticket_details():
    """Get detailed information for open tickets from Postgres"""
    def fetch_ticket_details():
        return get_open_ticket_details()

    try:
        details = get_cached_or_fetch("ticket_details", fetch_ticket_details)
        response = jsonify({"tickets": details})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        return response
    except Exception as e:
        log_print(f"Error in get_ticket_details endpoint: {e}")
        error_response = jsonify({"tickets": [], "error": f"Error fetching ticket details: {str(e)}"})
        error_response.headers.add('Access-Control-Allow-Origin', '*')
        return error_response

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