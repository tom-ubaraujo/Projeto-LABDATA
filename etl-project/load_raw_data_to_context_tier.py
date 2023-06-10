%idle_timeout 2880
%glue_version 3.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session #objeto spark dentro do contexto do glue

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# read files from raw-data
df_covid = spark.read.parquet("s3://data-lake-fia/raw-data/datasus-imunizacao")

df_covid.printSchema()

# change struct field to separeted columns
df_covid_final = (
    df_covid
    .select('_index',
            '_type',
            '_id',
            '_score',
            "_source.@timestamp",
            "_source.@version",
            "_source.co_condicao_maternal",
            "_source.data_importacao_datalake",
            "_source.data_importacao_rnds",
            "_source.document_id",
            "_source.ds_condicao_maternal",
            "_source.dt_deleted",
            "_source.estabelecimento_municipio_codigo",
            "_source.estabelecimento_municipio_nome",
            "_source.estabelecimento_razaoSocial",
            "_source.estabelecimento_uf",
            "_source.estabelecimento_valor",
            "_source.estalecimento_noFantasia",
            "_source.id_sistema_origem",
            "_source.paciente_dataNascimento",
            "_source.paciente_endereco_cep",
            "_source.paciente_endereco_coIbgeMunicipio",
            "_source.paciente_endereco_coPais",
            "_source.paciente_endereco_nmMunicipio",
            "_source.paciente_endereco_nmPais",
            "_source.paciente_endereco_uf",
            "_source.paciente_enumSexoBiologico",
            "_source.paciente_id",
            "_source.paciente_idade",
            "_source.paciente_nacionalidade_enumNacionalidade",
            "_source.paciente_racaCor_codigo",
            "_source.paciente_racaCor_valor",
            "_source.sistema_origem",
            "_source.status",
            "_source.vacina_categoria_codigo",
            "_source.vacina_categoria_nome",
            "_source.vacina_codigo",
            "_source.vacina_dataAplicacao",
            "_source.vacina_descricao_dose",
            "_source.vacina_fabricante_nome",
            "_source.vacina_fabricante_referencia",
            "_source.vacina_grupoAtendimento_codigo",
            "_source.vacina_grupoAtendimento_nome",
            "_source.vacina_lote",
            "_source.vacina_nome",
            "_source.vacina_numDose",
           )
)

# write parquet final into context tier
(
    df_covid_final
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://data-lake-fia/context/datasus_db/covid_dataset/")
)

job.commit()