import io
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType, ArrayType


SKILLS_LIST = [
    # Programming languages
    "python", "java", "scala", "sql", "r", "javascript", "typescript", "c++", "c#", "go",
    # Data & ML
    "spark", "databricks", "hadoop", "kafka", "airflow", "dbt", "mlflow",
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy", "pyspark",
    # Cloud & Warehousing
    "aws", "azure", "gcp", "snowflake", "redshift", "bigquery", "delta lake",
    # Databases
    "mysql", "postgresql", "mongodb", "redis", "elasticsearch", "oracle",
    # DevOps & Tools
    "git", "docker", "kubernetes", "jenkins", "terraform", "ci/cd",
    # BI & Analytics
    "tableau", "power bi", "looker", "excel",
    # Soft skills
    "leadership", "communication", "teamwork", "agile", "scrum",
]


@udf(returnType=StringType())
def parse_file_content(content, path):
    if content is None or path is None:
        return None
    try:
        path_lower = path.lower()
        if path_lower.endswith(".pdf"):
            from PyPDF2 import PdfReader
            reader = PdfReader(io.BytesIO(bytes(content)))
            text = ""
            for page in reader.pages:
                text += (page.extract_text() or "")
            return text.strip()
        elif path_lower.endswith(".docx"):
            from docx import Document
            doc = Document(io.BytesIO(bytes(content)))
            text = "\n".join([para.text for para in doc.paragraphs])
            return text.strip()
        else:
            return None
    except Exception as e:
        return f"PARSE_ERROR: {str(e)}"


@udf(returnType=StringType())
def extract_email(text):
    if text is None:
        return None
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(pattern, text)
    return match.group(0) if match else None


@udf(returnType=StringType())
def extract_phone(text):
    if text is None:
        return None
    # Remove PDF extraction artifacts: collapse multiple spaces into one
    text = re.sub(r' {2,}', ' ', text)
    # Matches formats: +1-800-555-1234, (800) 555-1234, 800.555.1234, 8005551234
    # \s? between digit groups handles spaces inserted by PDF extraction (e.g. "01 16")
    patterns = [
        r'\+?\d{1,3}[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\s?\d{4}',  # full international/US with optional space
        r'\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\s?\d{4}',                      # 10-digit US
        r'\+?\d{1,3}[\s\-.]?\d{3}[\s\-.]?\s?\d{4}',                       # short e.g. +1-555-0125
        r'\+?[\d\s\-\.]{7,15}',                                             # fallback: any digit sequence 7-15 chars
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    return None


@udf(returnType=ArrayType(StringType()))
def extract_skills(text):
    if text is None:
        return []
    text_lower = text.lower()
    found = [skill for skill in SKILLS_LIST if skill in text_lower]
    return list(dict.fromkeys(found))  # deduplicate while preserving order


@udf(returnType=BooleanType())
def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None
