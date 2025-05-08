#!/bin/bash

# Install dependencies
yum update -y
yum install -y python3-pip git

# Clone repository
git clone https://github.com/your-repo/bdi-final-assignment.git /app
cd /app

# Install Python requirements
pip3 install -r requirements.txt

# Set environment variables
cat > /app/.env << EOL
DB_HOST=${db_host}
DB_NAME=${db_name}
DB_USER=${db_user}
DB_PASSWORD=${db_password}
EOL

# Start FastAPI application
nohup uvicorn bdi_api.main:app --host 0.0.0.0 --port 80 &
