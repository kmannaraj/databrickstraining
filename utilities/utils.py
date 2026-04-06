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
    # 1. Look near phone/tel/mobile/cell keywords first — works for mocked data with any digit count
    keyword_match = re.search(
        r'(?:phone|tel|mobile|cell|contact)[^\d]{0,10}([\+\d][\d\s\-\.\(\)]{2,20})',
        text, re.IGNORECASE
    )
    if keyword_match:
        result = keyword_match.group(1).strip().rstrip('.,;')
        if len(re.sub(r'\D', '', result)) >= 3:
            return result
    # 2. Fallback: any digit sequence with optional separators, at least 3 digits
    fallback_match = re.search(r'\+?[\d][\d\s\-\.\(\)]{2,19}', text)
    if fallback_match:
        result = fallback_match.group(0).strip()
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
