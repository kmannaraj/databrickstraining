import dlt
from pyspark.sql.functions import (
    col, element_at, split, regexp_extract, current_timestamp,
)
from pyspark.sql import types as T
import sys

sys.path.append("../utilities")
from utils import parse_file_content, extract_email, extract_phone, extract_skills

catalog_name = spark.conf.get("catalog_name")
volume_path = "/Volumes/resume_batch3/staging/source_files/company_jds/"
primary_key = "path"


@dlt.view(name="company_jd_raw_vw")
def company_jd_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.{pdf,docx}")
        .load(volume_path)
        # File metadata
        .withColumn("file_name", element_at(split(col("path"), "/"), -1))
        .withColumn("folder_name", element_at(split(col("path"), "/"), -2))
        .withColumn("file_extension", regexp_extract(col("path"), r"\.([^.]+)$", 1))
        .withColumn("file_size_bytes", col("length"))
        # Timestamps
        .withColumn("upload_timestamp", col("modificationTime"))
        .withColumn("processing_timestamp", current_timestamp())
        # Parsed content
        .withColumn("parsed_content", parse_file_content(col("content"), col("path")))
        # Extracted fields
        .withColumn("extracted_email", extract_email(col("parsed_content")))
        .withColumn("extracted_phone", extract_phone(col("parsed_content")))
        .withColumn("extracted_skills", extract_skills(col("parsed_content")))
        .drop("content", "length")
    )
    return df


dlt.create_streaming_table(
    name=f"{catalog_name}.bronze.company_jd",
    comment="SCD1 Bronze target table - Company Job Description files",
)

dlt.apply_changes(
    target=f"{catalog_name}.bronze.company_jd",
    source="company_jd_raw_vw",
    keys=[primary_key],
    sequence_by=col("modificationTime"),
    stored_as_scd_type="1",
)
