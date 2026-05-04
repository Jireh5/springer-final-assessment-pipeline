# Use official Python image
FROM python:3.9-slim

# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure output directory exists
RUN mkdir -p /app/output

# Command to run both the profiler and the pipeline
CMD ["sh", "-c", "python data_profile.py && python your_script.py"]