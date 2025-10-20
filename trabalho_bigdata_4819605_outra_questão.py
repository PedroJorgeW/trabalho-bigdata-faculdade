from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Identificação pessoal
print("RU_do_aluno:", "4819605")

# 1️⃣ Inicialização da sessão Spark
spark = SparkSession.builder.appName("trabalho_big_data_4819605_enunciado1").getOrCreate()

# 2️⃣ Leitura do CSV
df = spark.read.csv(
    "/app/imdb-reviews-pt-br.csv",
    header=True,
    quote='"',
    escape='"',
    multiLine=True,
    encoding='UTF-8'
)

print("✅ Arquivo CSV carregado com sucesso.")

# 3️⃣ Filtrar apenas avaliações negativas
negativos_df = df.filter(col('sentiment') == 'neg')

# 4️⃣ Somar os IDs dos filmes negativos
# Assumindo que a coluna 'id' é numérica. Se estiver como string, converta com cast('int')
soma_ids = negativos_df.select(spark_sum(col('id').cast('int'))).collect()[0][0]

print("🔹 Soma de todos os IDs dos filmes negativos:", soma_ids)
