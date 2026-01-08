
Silent Data Failure Detector

Problem Statement

Most data pipelines fail silently.

Jobs succeed.
DAGs turn green.
Dashboards refresh.

But the data underneath can still be wrong.

Common real-world failures:
- Row counts drop unexpectedly
- Columns stop updating while pipelines keep running
- Categories quietly disappear
- Distributions drift slowly over time

These failures are often discovered late, after incorrect business decisions are already made.

This project detects silent data failures by modelling historical dataset behaviour and flagging deviations even when pipelines succeed.

What This Project Does

This is a batch data pipeline that:
1. Ingests real-world daily data
2. Profiles dataset health metrics
3. Builds historical baselines of normal behaviour
4. Detects deviations and records anomalies
5. Proves detection by intentionally breaking the data

The pipeline continues to run successfully, even as data quality degrades, and the system still detects the issue.

Architecture Overview

External API
↓
Raw Parquet (date-partitioned)
↓
Profiling Metrics
↓
Historical Baselines
↓
Anomaly Detection
↓
Anomaly Records

Key design decisions:
- Monitoring is separate from ingestion
- Pipelines are allowed to succeed
- Data trust is evaluated independently

Data Source

Source: Open-Meteo public weather API
Granularity: Hourly to daily batch
Entity: Single city
Storage format: Parquet (date-partitioned)

Example raw path:
data/raw/weather/date=YYYY-MM-DD/weather.parquet

Metrics Tracked

Each daily dataset is profiled using:
- Total row count
- Null percentage per column
- Mean values for numeric columns
- Distinct counts for categorical columns

Metrics are stored historically in:
data/metrics/weather_metrics.parquet

Baseline Modeling

Baselines are computed using rolling historical windows.

For each metric:
- Mean and standard deviation define normal
- Simple statistical thresholds are used
- Logic is intentionally explainable and conservative

Baselines are stored in:
data/baseline/weather_baseline.parquet

No machine learning is used. The goal is reliability and interpretability.

Anomaly Detection

Daily metrics are compared against historical baselines.

Anomalies are generated when:
- Metrics deviate significantly from historical norms
- Values become constant or frozen
- Categorical coverage drops unexpectedly

Detected anomalies are stored in:
data/anomalies/weather_anomalies.parquet

Each anomaly includes:
- Run date
- Metric name
- Observed value
- Reason code

Failure Simulation (Proof)

To validate the system, multiple silent failures were intentionally introduced without breaking the pipeline.

Simulated failures:
1. Volume drop (about 50 percent rows removed)
2. Frozen column (temperature forced constant)
3. Missing category (one weather category removed)

All failures were detected during ingestion and profiling, which were completed successfully.

Why This Matters

Most monitoring systems focus on:
- Job failures
- Exceptions
- Infrastructure health

This project focuses on:
- Data correctness
- Behavioural drift
- Trust in analytics

It models how real production failures happen quietly.

How to Run Locally

Requirements:
- Python 3.10
- Virtual environment
- Internet access

Run pipeline:
python ingestion/fetch_weather.py
python profiling/profile_weather.py
python baseline/compute_baseline.py
python detection/detect_anomalies.py

Design Principles

- Prefer explainable logic over opaque models
- Treat successful pipelines as potentially untrustworthy
- Separate ingestion from data health evaluation
- Optimise for the detection of slow, silent failures

Summary

This project demonstrates how to detect silent data failures in batch pipelines by modelling historical behaviour rather than trusting pipeline success.
