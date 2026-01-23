from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, current_date, current_timestamp, floor, months_between, concat_ws, avg
from pyspark.sql.types import StringType


spark = (
    SparkSession.builder
    .appName("teste_itau")
    .master("local[*]")
    .getOrCreate()
)


class DataQualityHandler:
    def __init__(self):
        pass
    
    
    def check_categorical_low_cardinality(
        self,
        df_silver: DataFrame,
        low_card_col: str,
        allowed_values: list[str],
    ) -> DataFrame:
        '''
        Check for not allowed values in a specific column
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param low_card_col: Low cardinality column to check for inconsistent values
        :type low_card_col: str
        :param allowed_values: List of allowed values into the low card col
        :type allowed_values: list[str]
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        df_silver_offensors = df_silver.where(~low_card_col(low_card_col).isin(allowed_values))\
            .withColumn("data_quality_offensor_cat", lit("categorical_domain"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(low_card_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(low_card_col).cast(StringType()).alias("offensor"),
                current_timestamp().alias("created_at"),
            )
        return df_silver_offensors


    def check_salary_discrepant_mean(
        self,
        df_silver: DataFrame,
        salary_col: str = "vl_renda",
        multiplier: float = 3.0,
    ) -> DataFrame:
        '''
        Check for discrepant salaries (too far from the salaries mean)
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param salary_col: Column that has salary information
        :type salary_col: str
        :param multiplier: The amount of discrepancy that we want to flag
        :type multiplier: float
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        salary_mean_value = (
            df_silver
            .select(avg(col(salary_col)).alias("mean_renda"))
            .collect()[0]["mean_renda"]
        )

        threshold = salary_mean_value * multiplier

        df_silver_offensors = df_silver\
            .where(col(salary_col) > lit(threshold))\
            .withColumn("data_quality_offensor_cat", lit("salary_discrepant_mean"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(salary_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(salary_col).cast(StringType()).alias("offensor"),
                lit(str(threshold)).alias("threshold"),
                current_timestamp().alias("created_at"),
            )
        return df_silver_offensors


    def check_negative_salary(
        self, 
        df_silver: DataFrame, 
        wage_col: str = "vl_renda"
    ) -> DataFrame:
        '''
        Check if there is any negativa salary at the clients wage column
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param wage_col: Column with clients wages
        :type wage_col: str
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        df_silver_offensors = df_silver.where(col(wage_col) < lit(0))\
            .withColumn("data_quality_offensor_cat", lit("negative_salary"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(wage_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(wage_col).cast(StringType()).alias("offensor"),
                current_timestamp().alias("created_at")
            )
        return df_silver_offensors


    def check_birth_future(
        self, 
        df_silver: DataFrame, 
        birth_day_col: str = "dt_nascimento_cliente"
    ) -> DataFrame:
        '''
        Check for futuristics birth days
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param birth_day_col: Column with clients birth days date 
        :type birth_day_col: str
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        df_silver_offensors = df_silver.where(col(birth_day_col) > current_date())\
            .withColumn("data_quality_offensor_cat", lit("birth_future_date"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(birth_day_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(birth_day_col).cast(StringType()).alias("offensor"),
                current_timestamp().alias("created_at")
            )
        return df_silver_offensors


    def check_nulls(
        self, 
        df_silver: DataFrame, 
        required_cols: list[str]
    ) -> DataFrame:
        '''
        Check if there is any null value in the required columns
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param required_cols: List with all the columns that can't have null values
        :type required_cols: list[str]
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        null_expression = None
        for required_col in required_cols:
            incremental_null_cond = col(required_col).isNull()
            null_expression = incremental_null_cond if null_expression is None else (null_expression | incremental_null_cond)

        df_silver_offensors = df_silver.where(null_expression)\
            .withColumn("data_quality_offensor_cat", lit("nulls_required"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(",".join(required_cols)).alias("analyzed_column"),
                col("cod_cliente"),
                lit(None).cast(StringType()).alias("offensor"),
                current_timestamp().alias("created_at")
            )
        return df_silver_offensors


    def check_phone_pattern(
        self,
        df_silver: DataFrame,
        phone_number_col: str = "num_telefone_cliente",
        phone_regex: str = r"^\(\d{2}\)\s?\d{4,5}-\d{4}$",
    ) -> DataFrame:
        '''
        Check if clients phone number match a wrong pattern
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param phone_number_col: Column with clients phone numbers
        :type phone_number_col: str
        :param phone_regex: Regex pattern to match inconsistent phone numbers
        :type phone_regex: str
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        # checar por DDDs específicos pode ser uma boa forma de incrementar a função
        # também podemos pensar em variedade mínima de número
        df_silver_offensors = df_silver.where(~col(phone_number_col).rlike(phone_regex))\
            .withColumn("data_quality_offensor_cat", lit("phone_pattern"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(phone_number_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(phone_number_col).cast(StringType()).alias("offensor"),
                lit(phone_regex).alias("expected_pattern"),
                current_timestamp().alias("created_at")
            )
        return df_silver_offensors


    def check_birth_coherent(
        self,
        df_silver: DataFrame,
        birth_day_col: str = "dt_nascimento_cliente",
        min_age: int = 18,
        max_age: int = 120,
    ) -> DataFrame:
        '''
        Check if clients birth day is in a valid interval
        
        :param df_silver: Silver DataFrame
        :type df_silver: DataFrame
        :param birth_day_col: Clients birth day column
        :type birth_day_col: str
        :param min_age: Minimum age we are supposed to see
        :type min_age: int
        :param max_age: Maximum age we are supposed to see
        :type max_age: int
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        age_years = floor(months_between(current_date(), col(birth_day_col)) / 12)
        
        df_silver_offensors = df_silver.where((age_years < lit(min_age)) | (age_years > lit(max_age)))\
            .withColumn("data_quality_offensor_cat", lit("birth_incoherent_age"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(birth_day_col).alias("analyzed_column"),
                col("cod_cliente"),
                col(birth_day_col).cast(StringType()).alias("offensor"),
                age_years.cast("int").alias("age_years"),
                lit(min_age).alias("min_age"),
                lit(max_age).alias("max_age"),
                current_timestamp().alias("created_at"),
            )
        return df_silver_offensors


    def check_address_exists(
        self,
        df_silver: DataFrame,
        address_ref_table: str,
        col_pais: str = "nm_pais_cliente",
        col_cidade: str = "nm_cidade_cliente",
        col_rua: str = "nm_rua_cliente",
        col_num: str = "num_casa_cliente",
    ) -> DataFrame:
        '''
        Check if the adresses exists using a complementar table with all valid adresses for certain locations
        
        :param df_silver: Silve DataFrame
        :type df_silver: DataFrame
        :param address_ref_table: Table with all valid adresses for certain cities/countries
        :type address_ref_table: str
        :param col_pais: Country's column
        :type col_pais: str
        :param col_cidade: City's column
        :type col_cidade: str
        :param col_rua: Street's column
        :type col_rua: str
        :param col_num: Street's number column
        :type col_num: str
        :return: Formatted DataFrame only with metadata related to the analysis and offensor rows
        :rtype: DataFrame
        '''
        df_valid_adresses = spark.read.format("delta").load(address_ref_table).select(
            col("col_pais").alias("pais"),
            col("col_cidade").alias("cidade"),
            col("col_rua").alias("rua"),
            col("col_num").alias("num"),
        ).dropDuplicates()

        df_silver_join = df_silver.select(
            "cod_cliente",
            col(col_pais).alias("pais"),
            col(col_cidade).alias("cidade"),
            col(col_rua).alias("rua"),
            col(col_num).alias("num"),
        )
        df_silver_offensors = df_silver_join.join(
            df_valid_adresses, 
            on=["pais", "cidade", "rua", "num"], 
            how="left_anti"
        )
        df_silver_offensors = df_silver_offensors\
            .withColumn("data_quality_offensor_cat", lit("address_not_found"))\
            .select(
                col("data_quality_offensor_cat"),
                lit(f"{col_pais}/{col_cidade}/{col_rua}/{col_num}").alias("analyzed_column"),
                col("cod_cliente"),
                concat_ws(" | ", "pais", "cidade", "rua", "num").alias("offensor"),
                current_timestamp().alias("created_at"),
            )
        return df_silver_offensors
       

# Exemplo de uso
# podemos pensar aqui em otimizar a leitura da silver, para avaliar somente casos novos/atualizados,
# utilizando a própria tabela de casos inconsistentes como base (ex.: pegar somente linhas da silver que não
# existam na tabela de inconsistência - )
df_silver = spark.read.format("delta").load("s3://bucket-teste-itau/silver/tb_clientes")

required = [
  "cod_cliente","nm_cliente","nm_pais_cliente","nm_cidade_cliente",
  "nm_rua_cliente","num_casa_cliente","telefone_cliente",
  "dt_nascimento_cliente","dt_atualizacao","tp_pessoa","vl_renda"
]

# uma melhoria geral aqui talvez seja incluir uma coluna só pra parâmetros particulares de cada
# regra, como uma "data_quality_confis", onde passo um dicionário com todos metadados utilizados para subsidiar
# a aplicação da regra em questão. Dessa forma preserva-se uma estrutura comum na saída de todas as funções e
# otimiza-se assim espaço de armazenamento

dqh = DataQualityHandler()

dq_violations = dqh.check_nulls(df_silver, required)\
    .unionByName(dqh.check_categorical_low_cardinality(df_silver, "tp_pessoa", ["PF","PJ"]))\
    .unionByName(dqh.check_negative_salary(df_silver, wage_col='vl_renda'))\
    .unionByName(dqh.check_salary_discrepant_mean(df_silver, multiplier=3.0))\
    .unionByName(dqh.check_birth_future(df_silver, birth_day_col="dt_nascimento_cliente"))\
    .unionByName(dqh.check_birth_coherent(df_silver, min_age=0, max_age=120))\
    .unionByName(dqh.check_phone_pattern(df_silver, phone_number_col="num_telefone_cliente", phone_regex=r"^\(\d{2}\)\s?\d{4,5}-\d{4}$"))\
    .unionByName(dqh.check_address_exists(df_silver, "s3://bucket-teste-itau/silver/tb_enderecos"))

# podemos persistir o df no lake... enviar para outro micro-serviço... etc.
