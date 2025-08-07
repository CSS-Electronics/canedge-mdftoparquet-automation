FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/CSS-Electronics/canedge-mdftoparquet-automation

WORKDIR /app

# Environment variables
# Note: CLOUD must be provided at runtime (Azure, Amazon, or Google)
ENV PYTHONUNBUFFERED=1
ENV MF4_DECODER=/app/mdf2parquet_decode

# Copy and install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the executable
COPY mdf2parquet_decode /app/mdf2parquet_decode
RUN chmod +x /app/mdf2parquet_decode

# Copy the Python modules and scripts
COPY modules/ /app/modules/
COPY process_backlog_container.py /app/

# Command to run when container starts
ENTRYPOINT ["python", "process_backlog_container.py"]
