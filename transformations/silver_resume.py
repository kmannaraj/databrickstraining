from pyspark.sql import functions as F
from pyspark import pipelines as dp

catalog_name = spark.conf.get("catalog_name", "resume_batch3")
primary_key = "path"
schema = "struct<Name:string, Title:string, Experience:string, Skills:string, Email:string, Phone_Number:string>"


@dp.view(name="silver_resume_vw")
def silver_resume():
    df = spark.readStream.table(
        f"{catalog_name}.bronze.resume_data"
    )
    df = df.selectExpr(
        """ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'You are a data extraction engine. Extract the following fields from the resume text:\n',
                '- Name (string): the candidate full name\n',
                '- Title (string): the candidate current or target job title\n',
                '- Experience (string, year range like "5-8" or "12+" or "12"): total years of experience\n',
                '- Skills (string): comma-separated list of technical skills\n',
                '- Email (string): the candidate email address\n',
                '- Phone_Number (string): any number labeled as phone, tel, mobile, or cell. This is mocked data — it may be as short as 3 digits. Extract the digits as-is, do not validate length.\n\n',
                'Rules:\n',
                '1. Return ONLY a valid JSON object, nothing else.\n',
                '2. No markdown, no code blocks, no explanations.\n',
                '3. Experience MUST be a string (e.g. "5-8", "12+", "3"). If only one number, keep as-is.\n',
                '4. Skills must be a comma-separated string without brackets.\n',
                '5. If a field is missing, return null for that field.\n\n',
                'Output format example:\n',
                '{\"Name\": \"John Smith\", \"Title\": \"Data Engineer\", \"Experience\": \"3-5\", \"Skills\": \"Python,Spark,SQL\", \"Email\": \"john@example.com\", \"Phone_Number\": \"555-01\"}\n\n',
                'Input text:\n---\n',
                parsed_content,
                '\n---\nJSON output:'
            )
        ) as data""",
        "modificationTime",
        "path"
    )
    df = df.select(
        F.from_json(F.col("data"), schema).alias("data"),
        F.col("path"),
        F.col("modificationTime")
    )
    df = df.select(
        F.col("data.*"),
        F.col("path"),
        F.col("modificationTime")
    )
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.silver.resume_data",
    comment="SCD1 Silver target table - Resume structured fields extracted via AI",
    expect_or_drop={
        "valid_name": "Name IS NOT NULL",
        "valid_skills": "Skills IS NOT NULL",
    },
)

dp.apply_changes(
    target=f"{catalog_name}.silver.resume_data",
    source="silver_resume_vw",
    keys=[primary_key],
    sequence_by="modificationTime",
    stored_as_scd_type="1",
)
