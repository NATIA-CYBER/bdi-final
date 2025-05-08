# Aircraft Tracking Infrastructure

This Terraform configuration sets up the complete infrastructure for the Aircraft Tracking application on AWS.

## Components

- VPC with public and private subnets
- EC2 instances for FastAPI and Airflow
- RDS PostgreSQL database
- S3 bucket for data storage
- Security groups for all components

## Prerequisites

1. AWS CLI installed and configured
2. Terraform installed
3. Access to an AWS account with necessary permissions

## Usage

1. Update `terraform.tfvars` with your desired values:
   ```hcl
   aws_region     = "your-region"
   s3_bucket_name = "your-bucket-name"
   db_username    = "your-db-username"
   db_password    = "your-db-password"
   ```

2. Initialize Terraform:
   ```bash
   terraform init
   ```

3. Review the changes:
   ```bash
   terraform plan
   ```

4. Apply the configuration:
   ```bash
   terraform apply
   ```

5. To destroy the infrastructure:
   ```bash
   terraform destroy
   ```

## Security Notes

1. Change the database credentials in `terraform.tfvars`
2. Consider restricting the CIDR blocks in security groups
3. Enable encryption for RDS and S3
4. Use AWS Secrets Manager for sensitive data in production

## Outputs

After successful deployment, you'll get:
- FastAPI server public IP
- Airflow server public IP
- RDS endpoint
- S3 bucket name
