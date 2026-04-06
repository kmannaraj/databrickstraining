from pyspark import pipelines as dp
from pyspark.sql.functions import col, element_at, split, regexp_extract, current_timestamp
from utilities import utils as u

# This file defines the Staging -> Bronze DLT pipeline (Lakeflow).
# catalog_name is passed as a pipeline parameter in the Lakeflow configuration.
catalog_name = spark.conf.get("catalog_name", "resume_batch3")

resume_volume_path    = f"/Volumes/{catalog_name}/staging/source_files/Resume_Data_PDF/"
company_jd_volume_path = f"/Volumes/{catalog_name}/staging/source_files/Company_JD_s/"

primary_key = "path"


# ---------------------------------------------------------------------------
# RESUMES
# ---------------------------------------------------------------------------

@dp.view(name="resume_data_vw")
def resume_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.{pdf,docx}")
        .load(resume_volume_path)
        # File metadata
        .withColumn("file_name", element_at(split(col("path"), "/"), -1))
        .withColumn("folder_name", element_at(split(col("path"), "/"), -2))
        .withColumn("file_extension", regexp_extract(col("path"), r"\.([^.]+)$", 1))
        .withColumn("file_size_bytes", col("length"))
        # Timestamps
        .withColumn("upload_timestamp", col("modificationTime"))
        .withColumn("processing_timestamp", current_timestamp())
        # Parsed content + extracted fields
        .withColumn("parsed_content", u.parse_file_content(col("content"), col("path")))
        .withColumn("extracted_email", u.extract_email(col("parsed_content")))
        .withColumn("extracted_phone", u.extract_phone(col("parsed_content")))
        .withColumn("extracted_skills", u.extract_skills(col("parsed_content")))
        .drop("content", "length")
    )
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.bronze.resume_data",
    comment="SCD1 Bronze target table - Resume files",
)

dp.create_auto_cdc_flow(
    target=f"{catalog_name}.bronze.resume_data",
    source="resume_data_vw",
    keys=[primary_key],
    sequence_by=col("modificationTime"),
    stored_as_scd_type="1",
)


# ---------------------------------------------------------------------------
# COMPANY JOB DESCRIPTIONS
# ---------------------------------------------------------------------------

@dp.view(name="company_jd_raw_vw")
def company_jd_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.{pdf,docx}")
        .load(company_jd_volume_path)
        # File metadata
        .withColumn("file_name", element_at(split(col("path"), "/"), -1))
        .withColumn("folder_name", element_at(split(col("path"), "/"), -2))
        .withColumn("file_extension", regexp_extract(col("path"), r"\.([^.]+)$", 1))
        .withColumn("file_size_bytes", col("length"))
        # Timestamps
        .withColumn("upload_timestamp", col("modificationTime"))
        .withColumn("processing_timestamp", current_timestamp())
        # Parsed content + extracted fields
        .withColumn("parsed_content", u.parse_file_content(col("content"), col("path")))
        .withColumn("extracted_email", u.extract_email(col("parsed_content")))
        .withColumn("extracted_phone", u.extract_phone(col("parsed_content")))
        .withColumn("extracted_skills", u.extract_skills(col("parsed_content")))
        .drop("content", "length")
    )
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.bronze.company_jd",
    comment="SCD1 Bronze target table - Company Job Description files",
)

dp.create_auto_cdc_flow(
    target=f"{catalog_name}.bronze.company_jd",
    source="company_jd_raw_vw",
    keys=[primary_key],
    sequence_by=col("modificationTime"),
    stored_as_scd_type="1",
)
