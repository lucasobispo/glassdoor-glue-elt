import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node bancos
bancos_node1691545870363 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://041334882807-source-trab1/Bancos/"],
        "recurse": True,
    },
    transformation_ctx="bancos_node1691545870363",
)

# Script generated for node reclamacao
reclamacao_node1691545569902 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ";",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://041334882807-source-trab1/Reclamacoes/"],
        "recurse": True,
    },
    transformation_ctx="reclamacao_node1691545569902",
)

# Script generated for node glassdoor
glassdoor_node1691545837513 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://041334882807-source-trab1/Empregados/"],
        "recurse": True,
    },
    transformation_ctx="glassdoor_node1691545837513",
)

# Script generated for node SQL Query
SqlQuery290 = """
SELECT
*,
    replace(UPPER(
REGEXP_REPLACE(
    REGEXP_REPLACE(

      REGEXP_REPLACE(

        REGEXP_REPLACE(

          REGEXP_REPLACE(

            REGEXP_REPLACE(

              LOWER(TRIM(' ' FROM REPLACE(Nome, ' (conglomerado)', ''))),

              '[áâãà]', 'a'),

            '[éê]', 'e'),

          '[íî]', 'i'),

        '[óôõ]', 'o'),

      '[úû]', 'u'),

    '[ç]', 'c')), '"','' )AS nome_tratado
FROM myDataSource;
"""
SQLQuery_node1691546818594 = sparkSqlQuery(
    glueContext,
    query=SqlQuery290,
    mapping={"myDataSource": bancos_node1691545870363},
    transformation_ctx="SQLQuery_node1691546818594",
)

# Script generated for node Drop Duplicates - reclamacao
DropDuplicatesreclamacao_node1691545963026 = DynamicFrame.fromDF(
    reclamacao_node1691545569902.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicatesreclamacao_node1691545963026",
)

# Script generated for node Drop Duplicates - glassdoor
DropDuplicatesglassdoor_node1691546265915 = DynamicFrame.fromDF(
    glassdoor_node1691545837513.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicatesglassdoor_node1691546265915",
)

# Script generated for node Change Schema - reclamacao
ChangeSchemareclamacao_node1691546009116 = ApplyMapping.apply(
    frame=DropDuplicatesreclamacao_node1691545963026,
    mappings=[
        ("cnpj if", "string", "cnpj if", "string"),
        ("instituição financeira", "string", "instituicao_financeira", "string"),
        ("índice", "string", "índice", "double"),
        (
            "quantidade total de reclamações",
            "string",
            "quantidade total de reclamações",
            "bigint",
        ),
        (
            "quantidade total de clientes – ccs e scr",
            "string",
            "quantidade total de clientes – ccs e scr",
            "bigint",
        ),
    ],
    transformation_ctx="ChangeSchemareclamacao_node1691546009116",
)

# Script generated for node Aggregate - glassdoor
Aggregateglassdoor_node1691546282210 = sparkAggregate(
    glueContext,
    parentFrame=DropDuplicatesglassdoor_node1691546265915,
    groups=["Nome"],
    aggs=[["Geral", "avg"], ["Remuneração e benefícios", "avg"]],
    transformation_ctx="Aggregateglassdoor_node1691546282210",
)

# Script generated for node SQL Query - normalizacao
SqlQuery289 = """
SELECT
*,
    replace(UPPER(
REGEXP_REPLACE(
    REGEXP_REPLACE(

      REGEXP_REPLACE(

        REGEXP_REPLACE(

          REGEXP_REPLACE(

            REGEXP_REPLACE(

              LOWER(TRIM(' ' FROM REPLACE(instituicao_financeira, ' (conglomerado)', ''))),

              '[áâãà]', 'a'),

            '[éê]', 'e'),

          '[íî]', 'i'),

        '[óôõ]', 'o'),

      '[úû]', 'u'),

    '[ç]', 'c')), '"','' )AS instituicao_financeira_tratada_sem_acentos_upper
FROM myDataSource;
"""
SQLQuerynormalizacao_node1691546132327 = sparkSqlQuery(
    glueContext,
    query=SqlQuery289,
    mapping={"myDataSource": ChangeSchemareclamacao_node1691546009116},
    transformation_ctx="SQLQuerynormalizacao_node1691546132327",
)

# Script generated for node Aggregate - reclamacao
Aggregatereclamacao_node1691546373177 = sparkAggregate(
    glueContext,
    parentFrame=SQLQuerynormalizacao_node1691546132327,
    groups=["instituicao_financeira_tratada_sem_acentos_upper", "cnpj if"],
    aggs=[
        ["índice", "avg"],
        ["quantidade total de reclamações", "sum"],
        ["quantidade total de clientes – ccs e scr", "sum"],
    ],
    transformation_ctx="Aggregatereclamacao_node1691546373177",
)

# Script generated for node Join - glassdoor-reclamacao
Joinglassdoorreclamacao_node1691546461402 = Join.apply(
    frame1=Aggregateglassdoor_node1691546282210,
    frame2=Aggregatereclamacao_node1691546373177,
    keys1=["Nome"],
    keys2=["instituicao_financeira_tratada_sem_acentos_upper"],
    transformation_ctx="Joinglassdoorreclamacao_node1691546461402",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691547441698 = ApplyMapping.apply(
    frame=Joinglassdoorreclamacao_node1691546461402,
    mappings=[
        ("Nome", "string", "right_Nome", "string"),
        ("`avg(Geral)`", "double", "`right_avg(Geral)`", "double"),
        (
            "`avg(Remuneração e benefícios)`",
            "double",
            "`right_avg(Remuneração e benefícios)`",
            "double",
        ),
        (
            "instituicao_financeira_tratada_sem_acentos_upper",
            "string",
            "right_instituicao_financeira_tratada_sem_acentos_upper",
            "string",
        ),
        ("cnpj if", "string", "cnpj if", "string"),
        ("`avg(índice)`", "double", "`right_avg(índice)`", "double"),
        (
            "`sum(quantidade total de reclamações)`",
            "bigint",
            "`sum(quantidade total de reclamações)`",
            "long",
        ),
        (
            "`sum(quantidade total de clientes – ccs e scr)`",
            "bigint",
            "`sum(quantidade total de clientes – ccs e scr)`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691547441698",
)

# Script generated for node Join
Join_node1691547391623 = Join.apply(
    frame1=RenamedkeysforJoin_node1691547441698,
    frame2=SQLQuery_node1691546818594,
    keys1=["right_Nome"],
    keys2=["Nome"],
    transformation_ctx="Join_node1691547391623",
)

# Script generated for node Change Schema
ChangeSchema_node1691547641072 = ApplyMapping.apply(
    frame=Join_node1691547391623,
    mappings=[
        ("right_Nome", "string", "nome _o_banco", "string"),
        (
            "`right_avg(Geral)`",
            "double",
            "indice_satisfacao_funcionario_geral",
            "double",
        ),
        (
            "`right_avg(Remuneração e benefícios)`",
            "double",
            "indice_satisfacao_salario",
            "double",
        ),
        ("cnpj if", "string", "cnpj", "string"),
        ("`right_avg(índice)`", "double", "indice_reclamcao", "double"),
        ("`sum(quantidade total de reclamações)`", "long", "total_reclamacao", "long"),
        (
            "`sum(quantidade total de clientes – ccs e scr)`",
            "long",
            "total_clientes",
            "long",
        ),
        ("Segmento", "string", "Segmento", "string"),
    ],
    transformation_ctx="ChangeSchema_node1691547641072",
)

# Script generated for node Amazon S3
AmazonS3_node1691548647554 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1691547641072,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://041334882807-target-trab1/resultado_final/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1691548647554",
)

job.commit()
