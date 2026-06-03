#!/usr/bin/env python3
"""
Benchmark script for chatbot endpoint.
Measures TTFT (Time To First Token) and total response time.
"""

import argparse
import json
import time
import sys
from datetime import datetime
from pathlib import Path
import requests
from typing import Dict, List, Tuple
from threading import Thread, Event, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# Default bearer token from the sample
DEFAULT_TOKEN = ""


# Test queries
# QUERIES = [
#     "Can you generate the data explorer link implement dynamic blacklisting for repeated login attempts",
#     "Can you generate the data explorer link to Check if the ransomware C2 domains are in our DNS blacklist."
# ]

# Test queries
QUERIES = [
    "Give me the data explorer link for failed login attempts from the ip 1.1.1.1",
    "Show me data explorer query for authentication failures from IP 192.168.1.100",
    "Create a data explorer link for suspicious login activity from 10.0.0.5",
    "Show me failed login attempts from China in the last 3 days",
    "Can you give the details of ip address 121.86.16.156 for past week",
    "Can you show me the details where login status is success",
    "Can you show me the details where login status is success and tags is approved-country",
    "Can you show me the details where login status is failure and tags is restricted-country",
    "Can you generate the application whitelisting enabled on the executive laptops",
    "Can you generate the data explorer link implement dynamic blacklisting for repeated login attempts",
    "Can you generate the data explorer link to Check if the ransomware C2 domains are in our DNS blacklist."
]

# API configuration
API_URL = "http://0.0.0.0:8080/converse-stream"
ORG_ID = "fbbb2585-14a0-47a2-b223-31403d497a6b"


def print_timer(stop_event: Event, start_time: float):
    """
    Print elapsed time every second until stop_event is set.
    """
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        # Use carriage return to overwrite the same line
        sys.stdout.write(f"\r    ⏱️  Elapsed: {elapsed:.1f}s")
        sys.stdout.flush()
        time.sleep(0.1)  # Check every 100ms for smoother updates


def make_request(query: str, token: str, silent: bool = False) -> Tuple[float, float, str, bool]:
    """
    Make a streaming request and measure timing metrics.

    Args:
        query: The query string to send
        token: Bearer token for authentication
        silent: If True, suppress all output (for concurrent mode)

    Returns:
        Tuple of (ttft_ms, total_time_ms, response_text, success)
    """
    headers = {
        'Content-Type': 'application/json',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'authorization': f'Bearer {token}',
        'content-type': 'application/json',
        'origin': 'https://dashboard.arcticwolf.com',
        'priority': 'u=1, i',
        'referer': 'https://dashboard.arcticwolf.com/',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site'
    }

    payload = {
        "messages": [
            {
                "role": "user",
                "text": query
            }
        ],
        "contextInfo": {
            "contextID": "main-dashboard",
            "conversationID": "",
            "organizationId": ORG_ID,
            "contextType": "UP",
            "invocationURL": f"https://dashboard.arcticwolf.com/{ORG_ID}",
            "asaFeatureConfig": {
                "sonnet_4_5": True,
                "up_expert": True,
                "disable_thinking_block": True
            }
        }
    }

    start_time = time.time()
    ttft = None
    response_text = ""
    success = True
    first_token_printed = False

    if not silent:
        print(f"    🚀 Sending request to {API_URL}...", flush=True)

    # Start the timer thread (only if not silent)
    stop_timer = Event()
    timer_thread = None
    if not silent:
        timer_thread = Thread(target=print_timer, args=(stop_timer, start_time), daemon=True)
        timer_thread.start()

    try:
        with requests.post(API_URL, headers=headers, json=payload, stream=True, timeout=120) as response:
            response.raise_for_status()

            for line in response.iter_lines():
                if line:
                    # Measure TTFT on first content
                    if ttft is None:
                        ttft = (time.time() - start_time) * 1000  # Convert to ms

                        if not silent:
                            # Stop timer and clear the line
                            stop_timer.set()
                            if timer_thread:
                                timer_thread.join(timeout=0.5)
                            sys.stdout.write("\r" + " " * 80 + "\r")  # Clear timer line
                            sys.stdout.flush()

                            # Print TTFT immediately
                            print(f"    ⚡ TTFT: {ttft:.2f}ms", flush=True)
                            print("    📝 Response: ", end='', flush=True)
                            first_token_printed = True

                    # Try to parse as JSON (streaming response format)
                    chunk_text = ""
                    try:
                        decoded = line.decode('utf-8')
                        if decoded.startswith('data: '):
                            decoded = decoded[6:]  # Remove 'data: ' prefix

                        if decoded.strip() and decoded.strip() != '[DONE]':
                            data = json.loads(decoded)
                            # Extract text from various possible response formats
                            if 'text' in data:
                                chunk_text = data['text']
                                response_text += chunk_text
                            elif 'content' in data:
                                chunk_text = data['content']
                                response_text += chunk_text
                            elif 'message' in data and 'content' in data['message']:
                                chunk_text = data['message']['content']
                                response_text += chunk_text
                    except (json.JSONDecodeError, UnicodeDecodeError, KeyError):
                        # If not JSON, treat as plain text
                        try:
                            chunk_text = line.decode('utf-8')
                            response_text += chunk_text
                        except:
                            pass

                    # Print all tokens as they arrive (only if not silent)
                    if chunk_text and not silent:
                        print(chunk_text, end='', flush=True)

    except requests.exceptions.RequestException as e:
        # Stop timer on error
        stop_timer.set()
        if timer_thread:
            timer_thread.join(timeout=0.5)
        if not silent:
            sys.stdout.write("\r" + " " * 80 + "\r")  # Clear timer line
            sys.stdout.flush()

        success = False
        response_text = f"Error: {str(e)}"
        if not silent:
            print(f"    ⚠️  Request failed: {e}")

    # Ensure timer is stopped
    if not stop_timer.is_set():
        stop_timer.set()
        if timer_thread:
            timer_thread.join(timeout=0.5)

    total_time = (time.time() - start_time) * 1000  # Convert to ms

    # If TTFT wasn't set (no content received), set it to total time
    if ttft is None:
        ttft = total_time
        if not silent:
            sys.stdout.write("\r" + " " * 80 + "\r")  # Clear timer line
            sys.stdout.flush()
            print(f"    ⚡ TTFT: {ttft:.2f}ms (no streaming content)", flush=True)

    # Print newline after streaming
    if first_token_printed and not silent:
        print()  # New line after streaming response

    # Print final summary
    if not silent:
        status_icon = "✓" if success else "✗"
        print(f"    {status_icon} Summary: TTFT={ttft:.2f}ms | Total={total_time:.2f}ms | Length={len(response_text)} chars", flush=True)

    return ttft, total_time, response_text, success


def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate the given percentile of a list of values."""
    if not values:
        return 0
    sorted_values = sorted(values)
    index = (len(sorted_values) - 1) * (percentile / 100)
    floor_index = int(index)
    ceil_index = floor_index + 1

    if ceil_index >= len(sorted_values):
        return sorted_values[floor_index]

    # Linear interpolation between floor and ceil
    fraction = index - floor_index
    return sorted_values[floor_index] + fraction * (sorted_values[ceil_index] - sorted_values[floor_index])


def calculate_stats(values: List[float]) -> Dict:
    """Calculate statistics for a list of values including percentiles."""
    if not values:
        return {'avg': 0, 'min': 0, 'max': 0, 'std': 0, 'p50': 0, 'p95': 0, 'p99': 0}

    avg = sum(values) / len(values)
    variance = sum((x - avg) ** 2 for x in values) / len(values)
    std = variance ** 0.5

    return {
        'avg': round(avg, 2),
        'min': round(min(values), 2),
        'max': round(max(values), 2),
        'std': round(std, 2),
        'p50': round(calculate_percentile(values, 50), 2),  # Median
        'p95': round(calculate_percentile(values, 95), 2),
        'p99': round(calculate_percentile(values, 99), 2)
    }


def run_benchmark_concurrent(token: str, iterations: int = 5, max_workers: int = 10) -> Dict:
    """
    Run benchmark with concurrent execution.
    All queries in a round are executed concurrently, then moves to the next round.

    Args:
        token: Bearer token for authentication
        iterations: Number of times to run each query (default: 5)
        max_workers: Maximum number of concurrent threads (default: 10)

    Returns:
        Dictionary with benchmark results
    """
    results = {
        'timestamp': datetime.now().isoformat(),
        'iterations': iterations,
        'queries': [],
        'execution_mode': 'concurrent'
    }

    # Initialize query results structure
    query_results = []
    for query in QUERIES:
        query_results.append({
            'query': query,
            'iterations': []
        })

    total_requests = len(QUERIES) * iterations
    request_count = 0
    print_lock = Lock()

    # Run in rounds: each round executes all queries concurrently
    for iteration in range(1, iterations + 1):
        print(f"\n{'='*80}")
        print(f"ROUND {iteration}/{iterations} (CONCURRENT MODE)")
        print(f"{'='*80}")
        print(f"Executing {len(QUERIES)} queries concurrently...")

        round_start_time = time.time()

        # Execute all queries in this round concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all queries for this round
            future_to_query = {}
            for query_idx, query in enumerate(QUERIES):
                future = executor.submit(make_request, query, token, True)  # silent=True
                future_to_query[future] = (query_idx, query)

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_query):
                query_idx, query = future_to_query[future]
                request_count += 1
                completed += 1

                try:
                    ttft, total, response, success = future.result()

                    with print_lock:
                        status_icon = "✓" if success else "✗"
                        print(f"  [{completed}/{len(QUERIES)}] {status_icon} Query {query_idx + 1}: "
                              f"TTFT={ttft:.2f}ms | Total={total:.2f}ms | {len(response)} chars")

                    query_results[query_idx]['iterations'].append({
                        'ttft_ms': round(ttft, 2),
                        'total_time_ms': round(total, 2),
                        'response_length': len(response),
                        'success': success,
                        'response': response,
                        'response_preview': response[:500] + ('...' if len(response) > 500 else '')
                    })
                except Exception as e:
                    with print_lock:
                        print(f"  [x] Query {query_idx + 1} failed with exception: {e}")

                    query_results[query_idx]['iterations'].append({
                        'ttft_ms': 0,
                        'total_time_ms': 0,
                        'response_length': 0,
                        'success': False,
                        'response': f"Exception: {str(e)}",
                        'response_preview': f"Exception: {str(e)}"
                    })

        round_time = time.time() - round_start_time
        print(f"\n{'─'*80}")
        print(f"Round {iteration} completed in {round_time:.2f}s")
        print(f"{'─'*80}")

        # Delay between rounds (except after the last round)
        if iteration < iterations:
            print(f"Starting round {iteration + 1}...")
            time.sleep(2)

    # Calculate statistics for each query
    print(f"\n{'='*80}")
    print("CALCULATING STATISTICS")
    print(f"{'='*80}")

    for query_idx, query_result in enumerate(query_results):
        ttfts = [it['ttft_ms'] for it in query_result['iterations'] if it['success']]
        totals = [it['total_time_ms'] for it in query_result['iterations'] if it['success']]
        success_count = sum(1 for it in query_result['iterations'] if it['success'])

        query_result['stats'] = {
            'ttft': calculate_stats(ttfts),
            'total_time': calculate_stats(totals),
            'success_rate': f"{success_count}/{iterations}",
            'success_percentage': round((success_count / iterations) * 100, 1)
        }

        print(f"\nQuery {query_idx + 1}: {query_result['query'][:60]}...")
        print(f"  📊 Success={query_result['stats']['success_rate']}")
        print(f"  ⚡ TTFT: Avg={query_result['stats']['ttft']['avg']}ms | "
              f"P50={query_result['stats']['ttft']['p50']}ms | "
              f"P95={query_result['stats']['ttft']['p95']}ms | "
              f"P99={query_result['stats']['ttft']['p99']}ms")
        print(f"  🏁 Total: Avg={query_result['stats']['total_time']['avg']}ms | "
              f"P50={query_result['stats']['total_time']['p50']}ms | "
              f"P95={query_result['stats']['total_time']['p95']}ms | "
              f"P99={query_result['stats']['total_time']['p99']}ms")

    results['queries'] = query_results
    return results


def run_benchmark(token: str, iterations: int = 5) -> Dict:
    """
    Run benchmark for all queries in a round-robin fashion.
    Cycles through all queries before starting the next iteration.

    Args:
        token: Bearer token for authentication
        iterations: Number of times to run each query (default: 5)

    Returns:
        Dictionary with benchmark results
    """
    results = {
        'timestamp': datetime.now().isoformat(),
        'iterations': iterations,
        'queries': [],
        'execution_mode': 'sequential'
    }

    # Initialize query results structure
    query_results = []
    for query in QUERIES:
        query_results.append({
            'query': query,
            'iterations': []
        })

    total_requests = len(QUERIES) * iterations
    request_count = 0

    # Run in rounds: each round executes all queries once
    for iteration in range(1, iterations + 1):
        print(f"\n{'='*80}")
        print(f"ROUND {iteration}/{iterations}")
        print(f"{'='*80}")

        for query_idx, query in enumerate(QUERIES):
            request_count += 1
            print(f"\n[{request_count}/{total_requests}] Query {query_idx + 1}/{len(QUERIES)} - Iteration {iteration}/{iterations}")
            print(f"  📋 {query}")

            ttft, total, response, success = make_request(query, token)

            query_results[query_idx]['iterations'].append({
                'ttft_ms': round(ttft, 2),
                'total_time_ms': round(total, 2),
                'response_length': len(response),
                'success': success,
                'response': response,  # Store full response
                'response_preview': response[:500] + ('...' if len(response) > 500 else '')  # Preview for HTML
            })


            # Small delay between requests (except for the last one)
            if request_count < total_requests:
                time.sleep(1)

        # Delay between rounds (except after the last round)
        if iteration < iterations:
            print(f"\n{'─'*80}")
            print(f"Round {iteration} complete. Starting round {iteration + 1}...")
            print(f"{'─'*80}")
            time.sleep(2)

    # Calculate statistics for each query
    print(f"\n{'='*80}")
    print("CALCULATING STATISTICS")
    print(f"{'='*80}")

    for query_idx, query_result in enumerate(query_results):
        ttfts = [it['ttft_ms'] for it in query_result['iterations'] if it['success']]
        totals = [it['total_time_ms'] for it in query_result['iterations'] if it['success']]
        success_count = sum(1 for it in query_result['iterations'] if it['success'])

        query_result['stats'] = {
            'ttft': calculate_stats(ttfts),
            'total_time': calculate_stats(totals),
            'success_rate': f"{success_count}/{iterations}",
            'success_percentage': round((success_count / iterations) * 100, 1)
        }

        print(f"\nQuery {query_idx + 1}: {query_result['query'][:60]}...")
        print(f"  📊 Success={query_result['stats']['success_rate']}")
        print(f"  ⚡ TTFT: Avg={query_result['stats']['ttft']['avg']}ms | "
              f"P50={query_result['stats']['ttft']['p50']}ms | "
              f"P95={query_result['stats']['ttft']['p95']}ms | "
              f"P99={query_result['stats']['ttft']['p99']}ms")
        print(f"  🏁 Total: Avg={query_result['stats']['total_time']['avg']}ms | "
              f"P50={query_result['stats']['total_time']['p50']}ms | "
              f"P95={query_result['stats']['total_time']['p95']}ms | "
              f"P99={query_result['stats']['total_time']['p99']}ms")

    results['queries'] = query_results
    return results


def generate_html_report(results: Dict, output_file: str):
    """
    Generate an HTML report from benchmark results.
    """
    iterations = results['iterations']
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chatbot Benchmark Results</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
        }}
        .timestamp {{
            opacity: 0.9;
            font-size: 0.9em;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin-top: 0;
            color: #667eea;
        }}
        .metric {{
            display: flex;
            justify-content: space-between;
            margin: 10px 0;
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }}
        .metric:last-child {{
            border-bottom: none;
        }}
        .metric-label {{
            font-weight: 500;
            color: #666;
        }}
        .metric-value {{
            font-weight: 600;
        }}
        .query-section {{
            background: white;
            padding: 25px;
            margin-bottom: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .query-header {{
            background: #f8f9fa;
            padding: 15px;
            border-left: 4px solid #667eea;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .query-text {{
            font-size: 1.1em;
            font-weight: 500;
            color: #333;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
            gap: 12px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            text-align: center;
        }}
        .stat-label {{
            font-size: 0.85em;
            color: #666;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 1.5em;
            font-weight: 600;
            color: #667eea;
        }}
        .iterations-table {{
            width: 100%;
            margin-top: 15px;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        .iterations-table th {{
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
            font-weight: 600;
        }}
        .iterations-table td {{
            padding: 8px 10px;
            border-bottom: 1px solid #eee;
        }}
        .iterations-table tr:hover {{
            background: #f8f9fa;
        }}
        .iterations-table tr:last-child td {{
            border-bottom: none;
        }}
        .status {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        .status.success {{
            background: #d4edda;
            color: #155724;
        }}
        .status.error {{
            background: #f8d7da;
            color: #721c24;
        }}
    </style>
</head>
<body>
"""

    execution_mode = results.get('execution_mode', 'sequential')
    mode_display = '🔄 Concurrent' if execution_mode == 'concurrent' else '➡️ Sequential'

    html += f"""
    <div class="header">
        <h1>🚀 Chatbot Benchmark Report</h1>
        <div class="timestamp">Generated: {results['timestamp']}</div>
        <div class="timestamp">{iterations} iterations per query</div>
        <div class="timestamp">Execution Mode: {mode_display}</div>
    </div>
"""

    # Calculate summary statistics
    total_queries = len(results['queries'])
    total_requests = total_queries * iterations

    # Calculate averages across all queries
    avg_ttft = sum(q['stats']['ttft']['avg'] for q in results['queries']) / total_queries
    avg_total = sum(q['stats']['total_time']['avg'] for q in results['queries']) / total_queries

    # Calculate overall success rate
    total_success = sum(
        int(q['stats']['success_rate'].split('/')[0])
        for q in results['queries']
    )
    success_pct = (total_success / total_requests) * 100

    # Calculate overall std dev
    all_ttfts = []
    all_totals = []
    for q in results['queries']:
        all_ttfts.extend([it['ttft_ms'] for it in q['iterations'] if it['success']])
        all_totals.extend([it['total_time_ms'] for it in q['iterations'] if it['success']])

    overall_ttft_stats = calculate_stats(all_ttfts)
    overall_total_stats = calculate_stats(all_totals)

    html += f"""
    <div class="summary">
        <div class="summary-card">
            <h3>📊 Overall Statistics</h3>
            <div class="metric">
                <span class="metric-label">Total Queries</span>
                <span class="metric-value">{total_queries}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Iterations per Query</span>
                <span class="metric-value">{iterations}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Total Requests</span>
                <span class="metric-value">{total_requests}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Success Rate</span>
                <span class="metric-value">{total_success}/{total_requests} ({success_pct:.1f}%)</span>
            </div>
        </div>

        <div class="summary-card">
            <h3>⚡ TTFT Statistics</h3>
            <div class="metric">
                <span class="metric-label">Average</span>
                <span class="metric-value">{avg_ttft:.2f}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P50 (Median)</span>
                <span class="metric-value">{overall_ttft_stats['p50']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P95</span>
                <span class="metric-value">{overall_ttft_stats['p95']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P99</span>
                <span class="metric-value">{overall_ttft_stats['p99']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Min (Best)</span>
                <span class="metric-value">{overall_ttft_stats['min']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Max (Worst)</span>
                <span class="metric-value">{overall_ttft_stats['max']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Std Deviation</span>
                <span class="metric-value">{overall_ttft_stats['std']}ms</span>
            </div>
        </div>

        <div class="summary-card">
            <h3>🏁 Total Time Statistics</h3>
            <div class="metric">
                <span class="metric-label">Average</span>
                <span class="metric-value">{avg_total:.2f}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P50 (Median)</span>
                <span class="metric-value">{overall_total_stats['p50']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P95</span>
                <span class="metric-value">{overall_total_stats['p95']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">P99</span>
                <span class="metric-value">{overall_total_stats['p99']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Min (Best)</span>
                <span class="metric-value">{overall_total_stats['min']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Max (Worst)</span>
                <span class="metric-value">{overall_total_stats['max']}ms</span>
            </div>
            <div class="metric">
                <span class="metric-label">Std Deviation</span>
                <span class="metric-value">{overall_total_stats['std']}ms</span>
            </div>
        </div>
    </div>
"""

    # Add detailed query results
    for i, query_result in enumerate(results['queries'], 1):
        stats = query_result['stats']

        html += f"""
    <div class="query-section">
        <div class="query-header">
            <div class="query-text">Query {i}: {query_result['query']}</div>
        </div>

        <div style="text-align: center; margin: 15px 0;">
            <strong>Success Rate:</strong> {stats['success_rate']} ({stats['success_percentage']}%)
        </div>

        <div style="margin: 20px 0;">
            <h4 style="color: #667eea; margin-bottom: 10px;">⚡ TTFT Statistics</h4>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Average</div>
                    <div class="stat-value">{stats['ttft']['avg']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P50</div>
                    <div class="stat-value">{stats['ttft']['p50']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P95</div>
                    <div class="stat-value">{stats['ttft']['p95']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P99</div>
                    <div class="stat-value">{stats['ttft']['p99']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Min</div>
                    <div class="stat-value">{stats['ttft']['min']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Max</div>
                    <div class="stat-value">{stats['ttft']['max']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Std Dev</div>
                    <div class="stat-value">{stats['ttft']['std']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
            </div>
        </div>

        <div style="margin: 20px 0;">
            <h4 style="color: #667eea; margin-bottom: 10px;">🏁 Total Time Statistics</h4>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Average</div>
                    <div class="stat-value">{stats['total_time']['avg']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P50</div>
                    <div class="stat-value">{stats['total_time']['p50']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P95</div>
                    <div class="stat-value">{stats['total_time']['p95']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P99</div>
                    <div class="stat-value">{stats['total_time']['p99']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Min</div>
                    <div class="stat-value">{stats['total_time']['min']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Max</div>
                    <div class="stat-value">{stats['total_time']['max']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Std Dev</div>
                    <div class="stat-value">{stats['total_time']['std']}<span style="font-size: 0.6em;">ms</span></div>
                </div>
            </div>
        </div>

        <details>
            <summary style="cursor: pointer; font-weight: 600; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                📋 View All {iterations} Iterations
            </summary>
            <table class="iterations-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>TTFT (ms)</th>
                        <th>Total Time (ms)</th>
                        <th>Response Length</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
"""

        for idx, iteration in enumerate(query_result['iterations'], 1):
            html += f"""
                    <tr>
                        <td><strong>{idx}</strong></td>
                        <td>{iteration['ttft_ms']}</td>
                        <td>{iteration['total_time_ms']}</td>
                        <td>{iteration['response_length']} chars</td>
                        <td><span class="status {'success' if iteration['success'] else 'error'}">{'✓ Success' if iteration['success'] else '✗ Error'}</span></td>
                    </tr>
"""

        html += """
                </tbody>
            </table>
        </details>
    </div>
"""

    html += """
</body>
</html>
"""

    with open(output_file, 'w') as f:
        f.write(html)

    print(f"\n✅ HTML report generated: {output_file}")


def generate_responses_file(results: Dict, output_file: str):
    """
    Generate a text file containing all queries and their responses.
    """
    iterations = results['iterations']

    execution_mode = results.get('execution_mode', 'sequential')

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("CHATBOT BENCHMARK - QUERIES AND RESPONSES\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {results['timestamp']}\n")
        f.write(f"Execution Mode: {execution_mode.upper()}\n")
        f.write(f"Iterations per query: {iterations}\n")
        f.write(f"Total queries: {len(results['queries'])}\n")
        f.write(f"Total requests: {len(results['queries']) * iterations}\n")
        f.write("="*80 + "\n\n")

        for i, query_result in enumerate(results['queries'], 1):
            f.write("\n" + "="*80 + "\n")
            f.write(f"QUERY {i}/{len(results['queries'])}\n")
            f.write("="*80 + "\n")
            f.write(f"{query_result['query']}\n")
            f.write("="*80 + "\n\n")

            stats = query_result['stats']
            f.write(f"📊 STATISTICS:\n")
            f.write(f"  Success Rate: {stats['success_rate']} ({stats['success_percentage']}%)\n")
            f.write(f"\n")
            f.write(f"  TTFT Statistics:\n")
            f.write(f"    Avg: {stats['ttft']['avg']}ms | P50: {stats['ttft']['p50']}ms | P95: {stats['ttft']['p95']}ms | P99: {stats['ttft']['p99']}ms\n")
            f.write(f"    Min: {stats['ttft']['min']}ms | Max: {stats['ttft']['max']}ms | StdDev: {stats['ttft']['std']}ms\n")
            f.write(f"\n")
            f.write(f"  Total Time Statistics:\n")
            f.write(f"    Avg: {stats['total_time']['avg']}ms | P50: {stats['total_time']['p50']}ms | P95: {stats['total_time']['p95']}ms | P99: {stats['total_time']['p99']}ms\n")
            f.write(f"    Min: {stats['total_time']['min']}ms | Max: {stats['total_time']['max']}ms | StdDev: {stats['total_time']['std']}ms\n")
            f.write("\n")

            for idx, iteration in enumerate(query_result['iterations'], 1):
                f.write("-"*80 + "\n")
                f.write(f"Iteration {idx}/{iterations}\n")
                f.write("-"*80 + "\n")
                f.write(f"Status: {'✓ SUCCESS' if iteration['success'] else '✗ FAILED'}\n")
                f.write(f"TTFT: {iteration['ttft_ms']}ms\n")
                f.write(f"Total Time: {iteration['total_time_ms']}ms\n")
                f.write(f"Response Length: {iteration['response_length']} characters\n")
                f.write("\n")
                f.write("RESPONSE:\n")
                f.write("-"*80 + "\n")
                f.write(iteration['response'])
                f.write("\n")
                f.write("-"*80 + "\n")
                f.write("\n")

        f.write("\n" + "="*80 + "\n")
        f.write("END OF REPORT\n")
        f.write("="*80 + "\n")

    print(f"✅ Responses file generated: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Benchmark chatbot endpoint with multiple iterations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings (5 iterations, sequential)
  python3 benchmark_chatbot.py

  # Run with 10 iterations per query (sequential)
  python3 benchmark_chatbot.py --iterations 10

  # Run with concurrent execution (all queries in a round run at the same time)
  python3 benchmark_chatbot.py --concurrent

  # Run concurrent with custom thread pool size
  python3 benchmark_chatbot.py --concurrent --max-workers 20 --iterations 5

  # Run with custom token and output files
  python3 benchmark_chatbot.py --token "YOUR_TOKEN" --output results.html --responses responses.txt

  # Quick concurrent test with 3 iterations
  python3 benchmark_chatbot.py --concurrent --iterations 3 --output quick_test.html
        """
    )
    parser.add_argument(
        '--token',
        default=DEFAULT_TOKEN,
        help='Bearer token for authentication (default: embedded token)'
    )
    parser.add_argument(
        '--output',
        default='benchmark_results.html',
        help='Output HTML file path (default: benchmark_results.html)'
    )
    parser.add_argument(
        '--responses',
        default='benchmark_responses.txt',
        help='Output text file for queries and responses (default: benchmark_responses.txt)'
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=5,
        help='Number of iterations per query (default: 5)'
    )
    parser.add_argument(
        '--concurrent',
        action='store_true',
        help='Execute all queries in each round concurrently instead of sequentially'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=10,
        help='Maximum number of concurrent threads when using --concurrent (default: 10)'
    )

    args = parser.parse_args()

    if args.iterations < 1:
        print("Error: iterations must be at least 1")
        return

    total_requests = len(QUERIES) * args.iterations
    execution_mode = "CONCURRENT" if args.concurrent else "SEQUENTIAL"

    print("="*80)
    print("🚀 Starting Chatbot Benchmark")
    print("="*80)
    print(f"Execution Mode: {execution_mode}")
    if args.concurrent:
        print(f"Max concurrent threads: {args.max_workers}")
    print(f"Testing {len(QUERIES)} queries")
    print(f"Iterations per query: {args.iterations}")
    print(f"Total requests: {total_requests}")
    print(f"API URL: {API_URL}")
    print(f"Organization ID: {ORG_ID}")
    print("="*80)
    print("\nNote: Flow type (traditional/agentic) is controlled by backend configuration")
    print("="*80)

    # Run benchmark
    start_time = time.time()
    if args.concurrent:
        results = run_benchmark_concurrent(args.token, args.iterations, args.max_workers)
    else:
        results = run_benchmark(args.token, args.iterations)
    elapsed_time = time.time() - start_time

    # Generate HTML report
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path(__file__).parent / output_path

    generate_html_report(results, str(output_path))

    # Generate responses text file
    responses_path = Path(args.responses)
    if not responses_path.is_absolute():
        responses_path = Path(__file__).parent / responses_path

    generate_responses_file(results, str(responses_path))

    print("\n" + "="*80)
    print("✅ Benchmark Complete!")
    print("="*80)
    print(f"Total time: {elapsed_time:.2f}s ({elapsed_time/60:.2f} minutes)")
    print(f"Average time per request: {elapsed_time/total_requests:.2f}s")
    print(f"\n📊 HTML Report: file://{output_path.absolute()}")
    print(f"📝 Responses File: file://{responses_path.absolute()}")


if __name__ == "__main__":
    main()
