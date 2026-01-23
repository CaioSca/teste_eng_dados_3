terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "glue_job_name" {
  type    = string
  default = "teste-eng-dados-etl-dq-tests"
}

variable "script_s3_path" {
  type        = string
  description = "S3 path do script principal do Glue Job (driver) que orquestra ETL + Data Quality + testes."
}

variable "temp_dir_s3_path" {
  type        = string
  description = "S3 path para TempDir do Glue Job (ex: s3://bucket/temp/glue/)."
}

variable "extra_py_files_s3_path" {
  type        = string
  default     = null
  description = "Opcional: S3 path de um .zip/.whl com dependencias/codigo (ex: s3://bucket/artifacts/project.zip)."
}

variable "additional_python_modules" {
  type        = string
  default     = null
  description = "Opcional: modulos Python para instalar (ex: pytest==7.4.4)."
}

variable "s3_data_buckets" {
  type        = list(string)
  default     = []
  description = "Lista de buckets S3 que o job precisa acessar (ex: [\"bucket-bronze\", \"bucket-silver\"])."
}

resource "aws_iam_role" "glue_job_role" {
  name = "${var.glue_job_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.glue_job_name}-s3-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [for b in var.s3_data_buckets : "arn:aws:s3:::${b}"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [for b in var.s3_data_buckets : "arn:aws:s3:::${b}/*"]
      }
    ]
  })
}

resource "aws_glue_job" "etl_dq_tests" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version       = "5.0"
  number_of_workers  = 10
  worker_type        = "G.1X"
  max_retries        = 0
  timeout            = 60
  execution_class    = "STANDARD"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = var.script_s3_path
  }

  default_arguments = merge(
    {
      "--job-language"                     = "python"
      "--enable-glue-datacatalog"          = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--TempDir"                          = var.temp_dir_s3_path
    },
    var.extra_py_files_s3_path != null ? { "--extra-py-files" = var.extra_py_files_s3_path } : {},
    var.additional_python_modules != null ? { "--additional-python-modules" = var.additional_python_modules } : {}
  )

  tags = {
    projeto = "teste_eng_dados"
  }
}

output "glue_job_name" {
  value = aws_glue_job.etl_dq_tests.name
}

output "glue_job_role_arn" {
  value = aws_iam_role.glue_job_role.arn
}
