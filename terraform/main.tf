# VPC and Networking
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
  
  tags = {
    Name = "Aircraft Data VPC"
  }
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
  
  tags = {
    Name = "Public Subnet"
  }
}

resource "aws_subnet" "private" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.2.0/24"
  
  tags = {
    Name = "Private Subnet"
  }
}

# Security Groups
resource "aws_security_group" "api" {
  name        = "api-security-group"
  description = "Security group for FastAPI application"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "airflow" {
  name        = "airflow-security-group"
  description = "Security group for Airflow"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  name        = "rds-security-group"
  description = "Security group for RDS"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.api.id, aws_security_group.airflow.id]
  }
}

# RDS Instance
resource "aws_db_instance" "postgres" {
  identifier           = "aircraft-data"
  allocated_storage    = 20
  storage_type        = "gp2"
  engine              = "postgres"
  engine_version      = "13.7"
  instance_class      = "db.t3.micro"
  name                = "airflow"
  username            = "airflow"
  password            = var.db_password
  skip_final_snapshot = true
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
}

resource "aws_db_subnet_group" "main" {
  name       = "aircraft-data"
  subnet_ids = [aws_subnet.private.id]
}

# EC2 Instances
resource "aws_instance" "api" {
  ami           = "ami-0c55b159cbfafe1f0" # Ubuntu 20.04 LTS
  instance_type = "t2.micro"
  
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.api.id]
  associate_public_ip_address = true
  
  user_data = templatefile("${path.module}/scripts/api_setup.sh", {
    db_host     = aws_db_instance.postgres.endpoint
    db_name     = aws_db_instance.postgres.name
    db_user     = aws_db_instance.postgres.username
    db_password = aws_db_instance.postgres.password
  })
  
  tags = {
    Name = "FastAPI Server"
  }
}

resource "aws_instance" "airflow" {
  ami           = "ami-0c55b159cbfafe1f0" # Ubuntu 20.04 LTS
  instance_type = "t2.medium" # More resources for Airflow
  
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  associate_public_ip_address = true
  
  user_data = templatefile("${path.module}/scripts/airflow_setup.sh", {
    db_host     = aws_db_instance.postgres.endpoint
    db_name     = aws_db_instance.postgres.name
    db_user     = aws_db_instance.postgres.username
    db_password = aws_db_instance.postgres.password
  })
  
  tags = {
    Name = "Airflow Server"
  }
}

# Variables
variable "db_password" {
  description = "Password for RDS instance"
  type        = string
  sensitive   = true
}

# Outputs
output "api_endpoint" {
  value = "http://${aws_instance.api.public_dns}"
}

output "airflow_endpoint" {
  value = "http://${aws_instance.airflow.public_dns}:8080"
}

output "rds_endpoint" {
  value = aws_db_instance.postgres.endpoint
}
