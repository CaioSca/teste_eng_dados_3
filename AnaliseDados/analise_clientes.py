from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, col, count, to_date, avg, months_between, lit


def identify_most_updated_clients(
    df: DataFrame,
    col_id: str = "cod_cliente",
) -> DataFrame:
    
    # dropar linhas duplicadas, antes de realizar a operação, pode nos levar a um resultado mais acurado
    df =  df.groupBy(col_id)\
           .agg(count(lit(1)).alias("qtd_atualizacoes"))\
           .orderBy(col("qtd_atualizacoes").desc(), col('cod_cliente').asc())\
           .limit(5)
    
    return df

# +-----------+----------------+
# |cod_cliente|qtd_atualizacoes|
# +-----------+----------------+
# |        396|               5|
# |        479|               5|
# |        878|               5|
# |        855|               4|
# |         71|               3|
# +-----------+----------------+

def identify_clients_age_average(
    df: DataFrame,
    col_dt_nasc: str = "dt_nascimento_cliente",
) -> DataFrame:
    
    dt = to_date(col(col_dt_nasc))
    
    idade = (months_between(current_date(), dt) / lit(12.0))
    
    df = df.select(avg(idade).alias("media_idade_anos"))

    return df

# +-----------------+
# |media_idade_anos|
# +-----------------+
# |50.69831182795328|
# +-----------------+