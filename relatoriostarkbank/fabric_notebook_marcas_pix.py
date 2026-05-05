"""
Fabric Notebook — Tabela Delta `marcas_chave_pix` no Lakehouse VestiLake.

Objetivo:
  - Manter cadastro de chaves PIX das marcas para a Vesti pagar antecipações.
  - Enriquecer com dados do Mongo (companyId, domainId) já presentes no lake.

Fonte das chaves: planilha "CC Starkbank - Walid"
  https://docs.google.com/spreadsheets/d/1S96Q04aMDThgEdQsD9VPq52aG7pDscg9eXrEupxXwRw/edit?gid=1979469094

Como usar:
  1. Abra um Fabric Notebook anexado ao Lakehouse VestiLake.
  2. Cole este conteúdo numa célula (ou mantenha como notebook).
  3. Rode. Atualize a lista PIX abaixo quando a planilha mudar.

Tabela final (overwrite a cada run): marcas_chave_pix
  nome_fantasia   string  -- nome da marca (case da planilha)
  chave_pix       string  -- chave PIX (CNPJ, CPF, email ou telefone)
  tipo_chave      string  -- CNPJ | CPF | EMAIL | TELEFONE
  cnpj            string  -- CNPJ informal (se houver)
  razao_social    string  -- razão social na planilha
  company_id      string  -- mongodb_companies._id (join por name)
  domain_id       bigint  -- mongodb_companies.domainId (join por name)
  fonte           string
  atualizado_em   timestamp
"""

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, TimestampType)

# -------- 1) Lista PIX (atualize aqui quando a planilha mudar) --------
# (nome_fantasia, chave_pix, tipo_chave, cnpj, razao_social)
PIX = [
    ("Kelly Rodrigues",     "54697378000115",            "CNPJ",  "54697378000115", "RK STORE LTDA"),
    ("Anne Blanc",          "43865993000177",            "CNPJ",  "43865993000177", "AMANDA CONFECCOES LTDA"),
    ("Bella Donna",         "11933990994",               "CPF",   "32805399000174", "BELLA DONNA MODA BRAS ARTIGOS DO VESTUARIO LTDA"),
    ("Barraca do Willinha", "35322340000113",            "CNPJ",  "35322340000113", "THAMIRIS ALTEA VIEIRA"),
    ("Evian",               "49238112000174",            "CNPJ",  "49238112000174", "LHB CONFECCOES DE ROUPAS LTDA"),
    ("Petit Enfant",        "60741324000102",            "CNPJ",  "60741324000102", "CORPORATIVO TACI LTDA"),
    ("Alcance Jeans",       "pessoal@alcancejeans.com",  "EMAIL", "34411241000145", "ALC INDUSTRIA E COMERCIO DE ROUPAS LTDA"),
    ("Incentive Moda",      "29780242000127",            "CNPJ",  "29780242000127", "CONFECCOES INCENTIVE LTDA"),
    ("Imporio Fitness",     "38280852000152",            "CNPJ",  "38280852000152", "ALINE LAYS DE LIMA SILVA TORRES - ME"),
    ("Arary",               "13495868000151",            "CNPJ",  "13495868000151", "ARARY COMERCIO DE TECIDOS LTDA"),
    ("BegKids",             "44870711000192",            "CNPJ",  "44870711000192", "ISABELA CARINY SALES MADEIRO - ME"),
    ("Ezee",                "moda.ezee4@gmail.com",      "EMAIL", "28314192000120", "EZEE CONFECCOES DE ROUPAS LTDA"),
    ("Tee Fashion",         "24459412000152",            "CNPJ",  "24459412000152", "TEE FASHION ARTIGOS DO VESTUARIO LTDA"),
    ("Zeros Confec",        "45180025000152",            "CNPJ",  "45180025000152", "ZEROS CONFECCOES LTDA"),
    ("Sedanbi",             "39922297000188",            "CNPJ",  "39922297000188", "COMERCIO E CONFECCOES DE ROUPAS S H LTDA"),
    ("MissMel",             "17974887000111",            "CNPJ",  "17974887000111", "GILMA MARIA SIMAO LTDA"),
    ("Oxigenio Modas",      "54911296000121",            "CNPJ",  "54911296000121", "GAIDO CONFECCOES LTDA"),
    ("Máfia Fitness",       "41549988000120",            "CNPJ",  "41549988000120", "LIMA CONFECCOES FITNESS LTDA"),
]

schema = StructType([
    StructField("nome_fantasia", StringType(), False),
    StructField("chave_pix",     StringType(), False),
    StructField("tipo_chave",    StringType(), False),
    StructField("cnpj",          StringType(), True),
    StructField("razao_social",  StringType(), True),
])
df_pix = spark.createDataFrame(PIX, schema=schema)

# -------- 2) Enriquece com mongodb_companies (companyId + domainId) --------
# Lakehouse VestiLake já contém mongodb_companies (mesma fonte dos relatórios CR/CP).
df_comp = (
    spark.table("mongodb_companies")
         .select(F.col("_id").alias("company_id"),
                 F.col("name").alias("nome_fantasia_mongo"),
                 F.col("domainId").alias("domain_id"))
)

# 1 marca pode ter múltiplos company_id (ex.: Bella Donna). Mantemos o de maior domain_id (mais recente).
w = (df_comp
     .withColumn("rn", F.row_number().over(
         __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .partitionBy(F.lower(F.col("nome_fantasia_mongo")))
            .orderBy(F.col("domain_id").desc_nulls_last())))
     .filter("rn = 1")
     .drop("rn"))

df_out = (df_pix
          .join(w, F.lower(df_pix["nome_fantasia"]) == F.lower(w["nome_fantasia_mongo"]), "left")
          .drop("nome_fantasia_mongo")
          .withColumn("fonte",         F.lit("CC Starkbank - Walid (gid=1979469094)"))
          .withColumn("atualizado_em", F.lit(datetime.now(timezone.utc)).cast(TimestampType()))
          .select("nome_fantasia","chave_pix","tipo_chave","cnpj","razao_social",
                  "company_id","domain_id","fonte","atualizado_em"))

# -------- 3) Grava Delta (overwrite — tabela é cadastro pequeno) --------
(df_out.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("marcas_chave_pix"))

print(f"OK — gravado {df_out.count()} marcas em marcas_chave_pix")
df_out.orderBy("nome_fantasia").show(truncate=False)
