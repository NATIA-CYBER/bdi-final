terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# S3 Bucket
resource "aws_s3_bucket" "aircraft_data" {
  bucket = "bdi-final-natia"  # Using your existing bucket name from .env

  tags = {
    Name = "Aircraft Data Bucket"
  }
}
