from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, input_file_name, current_timestamp, row_number, upper, regexp_extract, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.window import Window
from typing import Optional


spark = (
    SparkSession.builder
    .appName("teste_itau")
    .master("local[*]")
    .getOrCreate()
)

class ClientsETL:
    """
    Ingestion modes:
      - "d-1": process only events from the day before
      - "d-x": process events from a start_day (format YYYYMMDD) up to today
      - "full-refresh": process all available partitions under base_path_s3
    """
    def __init__(self,
        mode: str = "d-1",
        start_day: Optional[str] = None, 
        landzone_s3: str = "s3://bucket-teste-itau/landzone/trails_clientes",
        bronze_path: str = "s3://bucket-teste-itau/brz/trails_clientes",
        silver_path: str = "s3://bucket-teste-itau/silver/tb_clientes",
        ):
        '''
        Describing parameters passed through class instanciation
        
        :param mode: Ingestion mode
        :type mode: str
        :param start_day: Optional parameter. If you want to reprocess data from a given moment in time instead of d-1 or full-refresh
        :type start_day: Optional[str]
        :param landzone_s3: Landzone S3 path
        :type landzone_s3: str
        :param bronze_path: Bronze table path
        :type bronze_path: str
        :param silver_path: Silver table path
        :type silver_path: str
        '''
        self.mode = mode
        self.start_day = start_day
        self.landzone_s3 = landzone_s3
        self.bronze_path = bronze_path
        self.silver_path = silver_path

        self.base_path_s3 = "s3://bucket-teste-itau/landzone/trails_clientes" 

        today = datetime.now()
        today_yyyymmdd = today.strftime("%Y%m%d")
        
        # da pra pensar aqui em error handling personalizado pra cada input do usuário 
        # (mas como to assumindo que a pipe é triggada sistemicamente, to ignorando isso)
        if self.mode == "d-1":
            d1 = (today - timedelta(days=1)).strftime("%Y%m%d")
            self.extraction_days = [d1]

        elif self.mode == "d-x":
            start_extraction_day = datetime.strptime(self.start_day, "%Y%m%d")
            end_extraction_day = datetime.strptime(today_yyyymmdd, "%Y%m%d")

            self.extraction_days = []
            incrementing_start_extraction_day = start_extraction_day

            while incrementing_start_extraction_day <= end_extraction_day:
                self.extraction_days.append(incrementing_start_extraction_day.strftime("%Y%m%d"))
                incrementing_start_extraction_day += timedelta(days=1)

        elif self.mode == "full-refresh":
            self.extraction_days = [] 

        if self.mode == "full-refresh":
            self.input_paths = [f"{self.base_path_s3}/particao_*/**/*.csv"]
            self.anomesdia_processamento = None

        else:
            self.input_paths = [f"{self.base_path_s3}/particao_{anomesdia}/*.csv" for anomesdia in self.extraction_days]
            
            if len(self.extraction_days) == 1:
                self.anomesdia_processamento = self.extraction_days[0]
            else:
                self.anomesdia_processamento = None
        
    
    def extract_last_operation_metadata(self, delta_table_path: DeltaTable) -> dict:
        '''
        Standardize the metadata extraction related to operations related to certain delta table
        
        :param delta_table: The delta table you want to extract operations metadata from
        :type delta_table: DeltaTable
        :return: Metadata related to operations related to certain delta table
        :rtype: dict
        '''
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        dt_history = delta_table\
            .history(1)\
            .select(
                "version", 
                "timestamp", 
                "operation", 
                "operationMetrics"
            ).collect()[0]

        return {
            "version": dt_history["version"],
            "timestamp": dt_history["timestamp"],
            "operation": dt_history["operation"],
            "metrics": dt_history["operationMetrics"],
        }


    def ingest_csvs(self) -> DataFrame:
        '''
        Function responsible for ingesting new clients data
        
        :param self: Only parameters initialized with the class
        :return: Bronze dataframe with all the events (I / U / D) that happened with clients at specific ingestion window
        :rtype: DataFrame
        '''
        expeted_schema = StructType([
            StructField("cod_cliente", IntegerType(), False),
            StructField("nm_cliente", StringType(), False),
            StructField("nm_pais_cliente", StringType(), False),
            StructField("nm_cidade_cliente", StringType(), False),
            StructField("nm_rua_cliente", StringType(), False),
            StructField("num_casa_cliente", StringType(), False),
            StructField("telefone_cliente", StringType(), False),
            StructField("dt_nascimento_cliente", DateType(), False),
            StructField("dt_atualizacao", TimestampType(), False),
            StructField("tp_pessoa", StringType(), False),
            StructField("vl_renda", DecimalType(10, 2), False),
        ])

        df_brz = spark.read\
            .schema(expeted_schema) \
            .option("header", True)\
            .csv(self.input_paths)\
            .withColumn("anomesdia", regexp_extract(input_file_name(), r"particao_(\d{8})", 1))\
            .withColumn("arquivo_fonte", input_file_name())\
            .withColumn("dt_ingestao", current_timestamp())\
            .withColumn("nm_cliente", upper(col("nm_cliente")))\
            .withColumnRenamed("telefone_cliente", "num_telefone_cliente")

        df_brz = df_brz.distinct()

        return df_brz


    def update_bronze(self, df_brz: DataFrame) -> dict:
        '''
        Function responsible for updating bronze table with new clients events
        
        :param df_brz: Bronze dataframe with all the events (I / U / D) that happened with clients at specific ingestion window
        :type df_brz: DataFrame
        :return: Python dictionary with metadata related to the execution of saving process
        :rtype: dict
        '''
        bronze_path = "s3://bucket-teste-itau/brz/trails_clientes"

        # da pra pensar em cenários d-1 e d-x com mergeSchema True, pra evoluir o schema sem quebrar a pipeline
        if self.mode == "d-1":
            df_brz\
                .write\
                .format("delta")\
                .partitionBy("anomesdia")\
                .mode("append")\
                .save(bronze_path)

        elif self.mode == "d-x":
            replace_specific_partitions = "anomesdia IN ({})".format(
                ",".join([f"'{day}'" for day in self.target_days])
            )

            df_brz\
                .write\
                .format("delta")\
                .partitionBy("anomesdia")\
                .mode("overwrite")\
                .option("replaceWhere", replace_specific_partitions)\
                .save(bronze_path)
 
        elif self.mode == "full-refresh":
            df_brz\
                .write\
                .format("delta")\
                .mode("overwrite")\
                .option("overwriteSchema", "true")\
                .save(bronze_path)
            
        update_metadata = self.extract_last_operation_metadata(bronze_path)

        return update_metadata


    def update_silver(self) -> DataFrame:   
        if self.mode in ("d-1", "d-x"):    
            df_brz = spark\
                .read\
                .format("delta")\
                .load("s3://bucket-teste-itau/brz/trails_clientes")\
                .filter(col("anomesdia").isin(self.target_days))

        last_update = Window.partitionBy("cod_cliente").orderBy(col("dt_atualizacao").desc())
        df_new_brz_events = (
            df_brz
            .withColumn("row_num", row_number().over(last_update))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        df_new_brz_events = df_new_brz_events.withColumn(
            "num_telefone_cliente",
            when(
                col("num_telefone_cliente").rlike(r"^\(\d{2}\)\d{5}-\d{4}$"),
                col("num_telefone_cliente")
            ).otherwise(lit(None))
        )

        silver_path = "s3://bucket-teste-itau/silver/tb_clientes"

        silver_table = DeltaTable.forPath(spark, silver_path)

        # aqui daria pra colocarmos condições personalizadas e só realizar 
        # o update caso determinados campos fossem efetivamente atualizados ou não
        silver_table.alias("target")\
            .merge(
                df_new_brz_events.alias("source"),
                "target.cod_cliente = source.cod_cliente"
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
        
        merge_update_metadata = self.extract_last_operation_metadata("s3://bucket-teste-itau/silver/tb_clientes")

        return merge_update_metadata


    def execute_etl(self):
        df_brz = self.ingest_csvs()
        update_metadata = self.update_bronze(df_brz)
        update_metadata = self.update_silver()


# Exemplos de uso
if __name__ == "__main__":
    c_etl = ClientsETL(mode="d-1") # ou
    c_etl = ClientsETL(mode="d-x", start_day="20260121") # ou
    c_etl = ClientsETL(mode="full-refresh")

    c_etl.execute_etl()
