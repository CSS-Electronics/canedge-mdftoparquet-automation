FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/CSS-Electronics/canedge-mdftoparquet-automation

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the executable
COPY mdf2parquet_decode /app/mdf2parquet_decode
RUN chmod +x /app/mdf2parquet_decode

# Copy the Python modules and scripts
COPY modules/ /app/modules/
COPY process_backlog_azure.py /app/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV MF4_DECODER=/app/mdf2parquet_decode

# Command to run when container starts
ENTRYPOINT ["python", "process_backlog_azure.py"]
