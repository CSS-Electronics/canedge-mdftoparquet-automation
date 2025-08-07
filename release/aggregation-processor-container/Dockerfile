FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/CSS-Electronics/canedge-mdftoparquet-automation

WORKDIR /app

# Environment variables
# Note: CLOUD must be provided at runtime (Azure, Amazon, or Google)
ENV PYTHONUNBUFFERED=1

# Copy and install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python modules and scripts
COPY modules/ /app/modules/
COPY process_aggregation_container.py /app/

# Command to run when container starts
ENTRYPOINT ["python", "process_aggregation_container.py"]
