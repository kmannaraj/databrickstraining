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
    # Collapse multiple spaces (PDF artifact: "01 16" from "0116")
    text = re.sub(r' {2,}', ' ', text)
    # 1. Look near phone/tel/mobile/cell/contact keywords — captures whatever follows
    keyword_match = re.search(
        r'(?:phone|tel|mobile|cell|contact)\s*[:\-]?\s*([\+\d][\d\s\-\.\(\)]{1,25})',
        text, re.IGNORECASE
    )
    if keyword_match:
        result = keyword_match.group(1).strip().rstrip('.,; \n')
        # Trim trailing non-digit noise (e.g. captured extra word after number)
        result = re.sub(r'[\s\-\.]+$', '', result)
        if len(re.sub(r'\D', '', result)) >= 3:
            return result
    # 2. Fallback: any sequence starting with + or digit, allowing spaces/dashes, min 3 digits
    fallback_match = re.search(r'\+?[\d][\d\s\-\.\(\)]{2,24}', text)
    if fallback_match:
        result = fallback_match.group(0).strip().rstrip('.,;')
        if len(re.sub(r'\D', '', result)) >= 3:
            return result
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
