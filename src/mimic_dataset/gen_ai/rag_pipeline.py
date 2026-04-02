# Databricks notebook source
spark.sql("SHOW TABLES IN mimic_catalog.silver").show()

# COMMAND ----------

df = spark.read.table("mimic_catalog.silver.fact_admissions_enriched")

display(df)

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df = spark.read.table("mimic_catalog.silver.fact_admissions_enriched")

documents = df.select(
    concat_ws(
        " ",
        "subject_id",
        "admission_type",
        "diagnosis"
    ).alias("text")
)

display(documents)

# COMMAND ----------

documents.write.mode("overwrite").saveAsTable("mimic_catalog.gold.llm_documents")

# COMMAND ----------

spark.read.table("mimic_catalog.gold.llm_documents").show()

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %pip install langchain langchain-text-splitters

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

docs = spark.read.table("mimic_catalog.gold.llm_documents")

display(docs)

# COMMAND ----------

docs_pd = docs.toPandas()

texts = docs_pd["text"].tolist()

# COMMAND ----------

# DBTITLE 1,Cell 10
from langchain_text_splitters import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=200,
    chunk_overlap=20
)

chunks = []

for t in texts:
    chunks.extend(splitter.split_text(t))

# COMMAND ----------

chunk_df = spark.createDataFrame([(c,) for c in chunks], ["chunk_text"])

display(chunk_df)

# COMMAND ----------

chunk_df.write.mode("overwrite").saveAsTable("mimic_catalog.gold.llm_chunks")

# COMMAND ----------

spark.read.table("mimic_catalog.gold.llm_chunks").show()

# COMMAND ----------

# MAGIC %pip install databricks-genai-inference

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

chunks_df = spark.read.table("mimic_catalog.gold.llm_chunks")

display(chunks_df)

# COMMAND ----------

chunks_pd = chunks_df.toPandas()

texts = chunks_pd["chunk_text"].tolist()

# COMMAND ----------

# DBTITLE 1,Cell 18
from databricks_genai_inference import Embedding
import time

batch_size = 20
all_embeddings = []

for i in range(0, len(texts), batch_size):
    
    batch = texts[i:i+batch_size]
    
    response = Embedding.create(
        model="databricks-bge-large-en",
        input=batch
    )
    
    all_embeddings.extend(response.embeddings)
    
    time.sleep(1)   # prevents hitting rate limit

# COMMAND ----------

chunks_pd["embedding"] = all_embeddings

# COMMAND ----------

embeddings_df = spark.createDataFrame(chunks_pd)

embeddings_df.write.mode("overwrite").saveAsTable(
    "mimic_catalog.gold.llm_embeddings"
)

# COMMAND ----------

spark.read.table("mimic_catalog.gold.llm_embeddings").show()

# COMMAND ----------

# DBTITLE 1,Cell 22
# MAGIC %pip install databricks-vectorsearch
# MAGIC
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC
# MAGIC vsc = VectorSearchClient()
# MAGIC
# MAGIC # List existing vector search endpoints
# MAGIC endpoints = vsc.list_endpoints()
# MAGIC print(endpoints)

# COMMAND ----------

# DBTITLE 1,Cell 23
spark.sql("""
    ALTER TABLE mimic_catalog.gold.llm_embeddings
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

try:
    vsc.create_delta_sync_index(
        endpoint_name="mimic_vector_endpoint",
        source_table_name="mimic_catalog.gold.llm_embeddings",
        index_name="mimic_catalog.gold.mimic_llm_index",
        primary_key="chunk_text",
        pipeline_type="TRIGGERED",
        embedding_dimension=1024,
        embedding_vector_column="embedding"
    )
except Exception as e:
    if "already exists" in str(e):
        print("Index already exists, skipping creation.")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Cell 24
from databricks_genai_inference import Embedding

index = vsc.get_index(index_name="mimic_catalog.gold.mimic_llm_index")

# Wait for the index to be ready
index.wait_until_ready()

# Generate embedding for the query text
query_text = "patients admitted with pneumonia"
query_response = Embedding.create(
    model="databricks-bge-large-en",
    input=[query_text]
)
query_vector = query_response.embeddings[0]

results = index.similarity_search(
    query_vector=query_vector,
    columns=["chunk_text"],
    num_results=5
)

print(results)

# COMMAND ----------

# DBTITLE 1,Cell 25
from databricks_genai_inference import ChatCompletion

context = "\n".join([r[0] for r in results['result']['data_array']])

prompt = f"""
Answer the question using the context.

Context:
{context}

Question:
What diagnoses lead to emergency admission?
"""

response = ChatCompletion.create(
    model="databricks-meta-llama-3-3-70b-instruct",
    messages=[{"role":"user","content":prompt}]
)

print(response.message)

# COMMAND ----------

def ask_mimic_bot(question):

    # create embedding for the question
    query_response = Embedding.create(
        model="databricks-bge-large-en",
        input=[question]
    )

    query_vector = query_response.embeddings[0]

    # retrieve similar records
    results = index.similarity_search(
        query_vector=query_vector,
        columns=["chunk_text"],
        num_results=5
    )

    # build context
    context = "\n".join([r[0] for r in results['result']['data_array']])

    prompt = f"""
    Answer the question using the context.

    Context:
    {context}

    Question:
    {question}
    """

    response = ChatCompletion.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=[{"role":"user","content":prompt}]
    )

    return response.message

# COMMAND ----------

ask_mimic_bot("What are common emergency diagnoses?")

# COMMAND ----------

ask_mimic_bot("What is the average ICU stay for sepsis patients?")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(los) AS avg_icu_stay
# MAGIC FROM mimic_catalog.silver.fact_icustays
# MAGIC WHERE subject_id IN (
# MAGIC     SELECT subject_id
# MAGIC     FROM mimic_catalog.silver.fact_admissions_enriched
# MAGIC     WHERE diagnosis LIKE '%SEPSIS%'
# MAGIC )

# COMMAND ----------

ask_mimic_bot("What is the average ICU stay for sepsis patients?")

# COMMAND ----------

ask_mimic_bot("What diseases are frequently seen in emergency admissions?")

# COMMAND ----------

import pandas as pd
import re

text = ask_mimic_bot("What diseases are frequently seen in emergency admissions?")

# extract numbered list items
diseases = re.findall(r'\d+\.\s*(.*)', text)

df = pd.DataFrame(diseases, columns=["Common Emergency Diagnoses"])

display(df)

# COMMAND ----------

clean = [d.split("(")[0].strip() for d in diseases]

df = pd.DataFrame(clean, columns=["Disease"])

display(df)

# COMMAND ----------

def ask_mimic_bot_table(question):
    answer = ask_mimic_bot(question)

    diseases = re.findall(r'\d+\.\s*(.*)', answer)

    df = pd.DataFrame(diseases, columns=["Result"])

    display(df)

# COMMAND ----------

ask_mimic_bot_table("what diseases are frequently seen in emergency admissions?")

# COMMAND ----------

ask_mimic_bot("What heart-related conditions commonly lead to emergency admission?")

# COMMAND ----------

from databricks_genai_inference import ChatCompletion
import pandas as pd
import json

def ask_mimic_bot_table(question):

    query_response = Embedding.create(
        model="databricks-bge-large-en",
        input=[question]
    )

    query_vector = query_response.embeddings[0]

    results = index.similarity_search(
        query_vector=query_vector,
        columns=["chunk_text"],
        num_results=5
    )

    context = "\n".join([r[0] for r in results['result']['data_array']])

    prompt = f"""
    Answer the question using the context.

    Context:
    {context}

    Question:
    {question}

    IMPORTANT:
    Return the answer ONLY in JSON format like this:

    {{
      "results": [
        {{"item": "value"}}
      ]
    }}
    """

    response = ChatCompletion.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=[{"role": "user", "content": prompt}]
    )

    text = response.message

    data = json.loads(text)

    df = pd.DataFrame(data["results"])

    display(df)

# COMMAND ----------

ask_mimic_bot_table("What infections are common in emergency hospital admissions?")