# agent_utils.py (or top of notebook)

from pyspark.sql import functions as F
import json
import requests
import os

import requests
import json

def call_llm(prompt):
    api_key = "" # setup API key
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"

    headers = {
        "Content-Type": "application/json"
    }

    # Gemini uses a different JSON structure than OpenAI/OpenRouter
    payload = {
        "systemInstruction": {
            "parts": [{"text": "You are an expert PySpark data engineer."}]
        },
        "contents": [
            {
                "parts": [{"text": prompt}]
            }
        ],
        "generationConfig": {
            "temperature": 0.0 # Set to 0.0 for more predictable code generation
        }
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response_json = response.json()
        print("llm_response_11", response_json)

        # Gemini returns "candidates" instead of "choices"
        if "candidates" in response_json and len(response_json["candidates"]) > 0:
            code = response_json["candidates"][0]["content"]["parts"][0]["text"]
            print("code_11 ", code)
            print(len(code))
            return code
        else:
            # Print the error message from the API to debug
            print(f"API Error Response: {response_json}")
            return "# Error: LLM returned no candidates"
            
    except Exception as e:
        print(f"Request failed: {e}")
        return "# Error: Request failed"
    
def generate_data_profile(df):
    
    # Categorical unique values
    categorical_cols = [f.name for f in df.schema if f.dataType.simpleString() == "string"]
    unique_values = {
        col: [row[col] for row in df.select(col).distinct().limit(10).collect()]
        for col in categorical_cols
    }

    # Null counts
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in df.columns
    }

    # Numeric stats
    numeric_cols = [f.name for f in df.schema if f.dataType.simpleString() in ["int", "double", "float", "bigint"]]
    stats = df.select(numeric_cols).describe().toPandas().to_dict()
    result = {
        "unique_values": unique_values,
        "null_counts": null_counts,
        "stats": stats
    }
    print(result)
    return result

prompt = """
You are an expert PySpark data engineer building a Delta Live Tables (DLT) pipeline.

### Example:

Source Schema:
{{"trip_id": "string", "date": "date", "city_id": "string", "passenger_type": "string", "distance_travelled_km": "int", "fare_amount": "int", "passenger_rate": "int", "driver_rate": "int", "ingest_datetime": "timestamp", "file_name": "string", "_rescued_data": "string"}}

Target Schema:
{target_schema}

Expected Output Code:
from pyspark.sql import functions as F

# 1. Rename columns to match target schema
df = df.withColumnRenamed("trip_id", "id") \
       .withColumnRenamed("date", "business_date") \
       .withColumnRenamed("city", "city_id") \
       .withColumnRenamed("passenger_type", "passenger_category") \
       .withColumnRenamed("distance_travelled_km", "distance_kms") \
       .withColumnRenamed("fare_amount", "sales_amt") \
       .withColumnRenamed("passenger_rate", "passenger_rating") \
       .withColumnRenamed("driver_rate", "driver_rating") \
       .withColumnRenamed("ingest_datetime", "bronze_ingest_timestamp")

# 2. Handle Case-Sensitivity & Standardize Categorical Columns
df = df.withColumn("passenger_category", F.lower(F.col("passenger_category")))
df = df.withColumn("city_id", F.upper(F.col("city_id")))

# 3. Cast to Target Data Types (Strictly adhere to target schema)
df = df.withColumn("business_date", F.col("business_date").cast("date"))
df = df.withColumn("distance_kms", F.col("distance_kms").cast("int"))
df = df.withColumn("sales_amt", F.col("sales_amt").cast("int"))
df = df.withColumn("passenger_rating", F.col("passenger_rating").cast("int"))
df = df.withColumn("driver_rating", F.col("driver_rating").cast("int"))

# 4. Data Quality Validation & Flagging
valid_cities = ["RJ01", "UP01", "GJ01", "KL01", "MP01", "CH01", "GJ02", "AP01", "TN01", "KA01"]
valid_passengers = ["repeated", "new"]

# Collect failed column names into an array
df = df.withColumn(
    "failed_rules",
    F.array_remove(F.array(
        F.when(F.year(F.col("business_date")) < 2020, "business_date").otherwise(""),
        F.when(~F.col("driver_rating").between(1, 10), "driver_rating").otherwise(""),
        F.when(~F.col("passenger_rating").between(1, 10), "passenger_rating").otherwise(""),
        F.when(~F.col("city_id").isin(valid_cities), "city_id").otherwise(""),
        F.when(~F.col("passenger_category").isin(valid_passengers), "passenger_category").otherwise("")
    ), "")
)

# Generate JSON string flag: {{"status": "fail", "cols": "col1,col2"}}
df = df.withColumn(
    "quality_flag",
    F.when(
        F.size(F.col("failed_rules")) > 0,
        F.to_json(F.struct(
            F.lit("fail").alias("status"),
            F.array_join(F.col("failed_rules"), ",").alias("cols")
        ))
    ).otherwise(F.lit(None))
).drop("failed_rules")


### Now solve:

Source Schema:
{source_schema}

### Tasks:
1. **MAPPING & RENAMING (CRITICAL):** You MUST map every single source column to its corresponding Target Schema column using `.withColumnRenamed()`. Use semantic similarity (e.g. `fare` -> `sales_amt`, `rate` -> `rating`). Do NOT skip any target columns.
2. Standardize string casing (convert `passenger_category` to lowercase).
3. Cast data types accurately. Distance, amounts, and ratings must be `integer`. Date must be `date`. Ensure you cast the *renamed* columns, not the old source columns.
4. Create a JSON flag column named `quality_flag` exactly matching the format `{{'status': 'fail', 'cols':'failed_col_names'}}` based on the following strict rules:
   - `year(business_date) >= 2020`
   - `driver_rating` BETWEEN 1 AND 10
   - `passenger_rating` BETWEEN 1 AND 10
   - `city_id` IN ["RJ01", "UP01", "GJ01", "KL01", "MP01", "CH01", "GJ02", "AP01", "TN01", "KA01"]
   - `passenger_category` IN ["repeated", "new"]

### Rules:
- Use only PySpark syntax (`pyspark.sql.functions as F`).
- Use `df` as the input dataframe and output dataframe.
- Do NOT create a new dataframe variable.
- Return ONLY the executable PySpark code block. Do NOT wrap it in markdown blockquotes like ```python.
"""

def agent_transform(df, target_schema: dict):
    import json
    source_schema = {f.name: str(f.dataType) for f in df.schema}
    final_prompt=prompt.format(source_schema=json.dumps(source_schema), target_schema=json.dumps(target_schema))
    generated_code = call_llm(final_prompt)

    # exec() will instantly crash if it sees those backticks.
    clean_code = generated_code
    if "```python" in clean_code:
        clean_code = clean_code.split("```python")[1]
    if "```" in clean_code:
        clean_code = clean_code.split("```")[0]
        
    clean_code = clean_code.strip()

    # 2. ISOLATED EXECUTION SCOPE
    exec_scope = {
        "df": df,
        "F": F
    }

    # 3. THE FAIL-FAST BLOCK
    try:
        exec(clean_code, globals(), exec_scope)
        df_transformed = exec_scope["df"]
        return df_transformed
        
    except Exception as e:
        error_msg = (
            f"AI Agent Transformation Failed!\n"
            f"Python Error: {str(e)}\n"
            f"--------------------------\n"
            f"Code Attempted to Execute:\n{clean_code}"
        )
        raise RuntimeError(error_msg)