# Dockerfile for Mutual Fund Alpha Decomposition Tool

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional packages that might be needed
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY mutual_fund_alpha/ ./mutual_fund_alpha/
COPY main.py .

# Copy data directory
COPY data/ ./data/

# Expose port for Streamlit
EXPOSE 8501

# Set environment variables
ENV PYTHONPATH=/app

# Command to run the Streamlit app
CMD ["streamlit", "run", "mutual_fund_alpha/src/dashboard/app.py"]