# Start from the official Airflow image with Python 3.10
# WHY this base image? It comes with Airflow pre-installed and configured.
# We just need to add Spark and Delta Lake on top.
FROM apache/airflow:2.9.0-python3.10

# Switch to root to install system packages
USER root

# Install Java — Spark is built on the JVM and won't run without it
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Switch back to airflow user (security best practice — don't run as root)
USER airflow

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
