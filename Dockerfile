FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ .

# Copy test scripts
COPY test_*.py ./
COPY clear_*.py ./

# Copy scripts to normalize and interpolate data
COPY interpolate_*.py ./
COPY normalize_*.py ./

# Copy migration script
COPY migrate_add_timestamp_sql.py ./
COPY migrate_deduplicate_and_unique.py ./

# Create necessary directories
RUN mkdir -p /data /logs

# Make scripts executable
RUN chmod +x main.py

# Expose API port
EXPOSE 9714

# Replace the CMD to run the migration and then start the API
CMD ["sh", "-c", "python3 migrate_deduplicate_and_unique.py && uvicorn api:app --host 0.0.0.0 --port 9714"] 