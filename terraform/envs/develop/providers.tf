###############################################################################
# providers.tf
#
# Declares the cloud providers Terraform needs and pins their versions.
# Version pinning is critical in production — without it, a future provider
# release could silently change resource behavior and break the pipeline.
###############################################################################

terraform {
  # Minimum Terraform CLI version. Anything below 1.5 lacks some of the
  # validation features and lifecycle blocks used in this project.
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Allow 5.x patches/minors, block major upgrades
    }

    # archive provider zips Lambda source files locally before upload
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

# AWS provider configuration.
# Region and profile are sourced from variables so the same code can be
# deployed to multiple environments without editing this file.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  # Default tags are applied to every taggable resource Terraform creates.
  # This is invaluable for cost allocation, ownership tracking, and cleanup.
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = var.owner
    }
  }
}
