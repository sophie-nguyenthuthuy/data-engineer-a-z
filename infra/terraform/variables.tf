variable "region" {
  description = "AWS region (Singapore gần Việt Nam nhất)."
  type        = string
  default     = "ap-southeast-1"
}

variable "bucket_name" {
  description = "Tên bucket S3 — phải duy nhất toàn cầu, ví dụ dea2z-lake-<tên bạn>."
  type        = string
}

variable "glue_database" {
  description = "Tên Glue database."
  type        = string
  default     = "coffee_lake"
}

variable "name_prefix" {
  description = "Prefix cho IAM resource."
  type        = string
  default     = "dea2z"
}

variable "owner" {
  description = "Tag owner."
  type        = string
  default     = "learner"
}

variable "bronze_retention_days" {
  description = "Số ngày giữ dữ liệu bronze trước khi tự xoá."
  type        = number
  default     = 90
}
