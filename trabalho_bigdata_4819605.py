# ---------------------------------------------
# Trabalho Prático de Big Data - RU 4819605
# ---------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, split, size
import re

# Identificação pessoal
print("RU_do_aluno:", "4819605")

# ---------------------------------------------
# 1️⃣ Inicializar sessão Spark
# ---------------------------------------------
spark = SparkSession.builder.appName("trabalho_big_data_4819605_enunciado2").getOrCreate()

# ---------------------------------------------
# 2️⃣ Leitura do arquivo CSV
# ---------------------------------------------
df = spark.read.csv(
    "/app/imdb-reviews-pt-br.csv",
    header=True,
    quote='"',
    escape='"',
    multiLine=True,
    encoding='UTF-8'
)

print("✅ Arquivo CSV carregado com sucesso.")
print("Colunas do arquivo:", df.columns)

# ---------------------------------------------
# 3️⃣ Filtrar apenas avaliações negativas
# ---------------------------------------------
negativos_df = df.filter(df["sentiment"] == "neg")

# ---------------------------------------------
# 4️⃣ Contar palavras em cada idioma
# ---------------------------------------------
# Conta palavras nos textos em inglês e português
negativos_df = negativos_df.withColumn("word_count_en", size(split(lower(df["text_en"]), "\\s+")))
negativos_df = negativos_df.withColumn("word_count_pt", size(split(lower(df["text_pt"]), "\\s+")))

# ---------------------------------------------
# 5️⃣ Somar total de palavras por idioma
# ---------------------------------------------
totais = negativos_df.selectExpr(
    "sum(word_count_en) as total_en",
    "sum(word_count_pt) as total_pt"
).collect()[0]

total_en = totais["total_en"]
total_pt = totais["total_pt"]
diferenca = total_pt - total_en

# ---------------------------------------------
# 6️⃣ Exibir resultados finais
# ---------------------------------------------
print("\n=== RESULTADOS ===")
print(f"📊 Total de palavras em textos negativos (Inglês): {total_en}")
print(f"📊 Total de palavras em textos negativos (Português): {total_pt}")
print(f"✅ Diferença (Português - Inglês): {diferenca}")

# ---------------------------------------------
# 7️⃣ Encerrar sessão Spark
# ---------------------------------------------
spark.stop()
print("\n✅ Execução concluída com sucesso - RU 4819605 ✅")
