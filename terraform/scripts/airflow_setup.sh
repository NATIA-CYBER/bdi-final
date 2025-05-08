#!/bin/bash

# Install dependencies
yum update -y
yum install -y python3-pip git docker

# Start Docker service
systemctl start docker
systemctl enable docker

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Clone repository
git clone https://github.com/your-repo/bdi-final-assignment.git /app
cd /app

# Set environment variables
cat > /app/.env << EOL
DB_HOST=${db_host}
DB_NAME=${db_name}
DB_USER=${db_user}
DB_PASSWORD=${db_password}
S3_BUCKET=${s3_bucket}
EOL

# Start Airflow using Docker Compose
docker-compose -f docker-compose.yaml up -d
