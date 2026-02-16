# Dockerfile for DWH Runner
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install system and Python dependencies
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    pip install --no-cache-dir psycopg2-binary python-dotenv

# Default command to keep container running
CMD ["tail", "-f", "/dev/null"]
