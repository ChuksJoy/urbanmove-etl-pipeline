FROM apache/airflow:2.10.3

# Switch to the airflow user
USER airflow

# Copy your requirements
COPY --chown=airflow:root requirements.txt /requirements.txt

# REMOVED --user flag to work with the internal venv
RUN pip install --no-cache-dir -r /requirements.txt

# Set the path so your scripts are always found
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"
