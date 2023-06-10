-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # import libraries
-- MAGIC import pyspark.sql.functions as f
-- MAGIC import urllib

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Credentials to access data-lake-fia bucket on s3
-- MAGIC ACCESS_KEY = "AKIAQJ2IVF6JBGLXOBEF"
-- MAGIC SECRET_KEY = "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"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Encoded secret key for security purposes
-- MAGIC ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, safe="")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # variables used to mount drive
-- MAGIC AWS_S3_BUCKET = 'data-lake-fia'
-- MAGIC MOUNT_NAME = '/mnt/data-lake-fia'
-- MAGIC SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}"
-- MAGIC
-- MAGIC # Mount the drive
-- MAGIC if all(mount.mountPoint != MOUNT_NAME for mount in dbutils.fs.mounts()):
-- MAGIC      dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
-- MAGIC         
-- MAGIC         
-- MAGIC # view content of s3 bucket
-- MAGIC display(dbutils.fs.ls(MOUNT_NAME))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Define location of parquet files (raw-data and context tier)
-- MAGIC raw_tier_files = "/mnt/data-lake-fia/raw-data/datasus-imunizacao/"
-- MAGIC context_tier_files = "/mnt/data-lake-fia/context/datasus_db/covid_dataset/"
-- MAGIC
-- MAGIC # create Dataframe to make analysis
-- MAGIC df_covid = spark.read.parquet(context_tier_files)
-- MAGIC df_covid.display()
-- MAGIC
-- MAGIC # view schema
-- MAGIC df_covid.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_covid.write.format("delta").mode("overwrite").save("/marcelonjunior/delta/covid")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --CRIA A TABELA IMUNIZAÇÃO A PARTIR DOS DADOS DO S3
-- MAGIC DROP TABLE IF EXISTS imunizacao;
-- MAGIC CREATE TABLE imunizacao USING DELTA LOCATION '/marcelonjunior/delta/covid/'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC     paciente_racaCor_valor AS Raca,
-- MAGIC     count(DISTINCT paciente_id) AS Pacientes
-- MAGIC FROM
-- MAGIC   imunizacao
-- MAGIC GROUP BY
-- MAGIC   paciente_racaCor_valor
-- MAGIC ORDER BY
-- MAGIC   Pacientes desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC     paciente_endereco_uf AS UF,
-- MAGIC     count(DISTINCT paciente_id) AS Pacientes
-- MAGIC FROM
-- MAGIC   imunizacao 
-- MAGIC GROUP BY
-- MAGIC   paciente_endereco_uf
-- MAGIC ORDER BY
-- MAGIC   Pacientes desc 
-- MAGIC LIMIT 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC     paciente_endereco_uf AS UF,
-- MAGIC     count(DISTINCT (CASE WHEN paciente_enumSexoBiologico = 'M' THEN paciente_id END)) AS qt_Paciente_M,
-- MAGIC     count(DISTINCT (CASE WHEN paciente_enumSexoBiologico = 'F' THEN paciente_id END)) AS qt_Paciente_F,
-- MAGIC     count(DISTINCT  paciente_id) AS qt_Paciente_TT
-- MAGIC     
-- MAGIC FROM
-- MAGIC   imunizacao 
-- MAGIC WHERE paciente_enumSexoBiologico <> 'I'
-- MAGIC GROUP BY
-- MAGIC   paciente_endereco_uf
-- MAGIC ORDER BY qt_Paciente_TT DESC
-- MAGIC LIMIT 10
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC     LEFT(vacina_dataAplicacao,7) AS PERIODO,
-- MAGIC     count( (CASE WHEN paciente_enumSexoBiologico = 'M' THEN document_id END)) AS qt_Paciente_M,
-- MAGIC     count( (CASE WHEN paciente_enumSexoBiologico = 'F' THEN document_id END)) AS qt_Paciente_F,
-- MAGIC     count(  document_id) AS qt_Paciente_TT
-- MAGIC     
-- MAGIC FROM
-- MAGIC   imunizacao 
-- MAGIC GROUP BY
-- MAGIC    LEFT(vacina_dataAplicacao,7)
-- MAGIC ORDER BY qt_Paciente_TT DESC
-- MAGIC
-- MAGIC

-- COMMAND ----------


