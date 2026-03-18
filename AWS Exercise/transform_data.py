from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# 1. START THE SPARK ENGINE
# We use a SparkSession, which is the entry point for all Spark functionality
spark = SparkSession.builder \
    .appName("AdventureWorksTransformation") \
    .getOrCreate()


def run_transformation():
    try:
        print("\n--- Starting Spark Transformation ---")

        # 2. LOAD: Read our local 'Staged' CSVs (Bronze Layer)
        # We use inferSchema=True so Spark automatically detects numbers vs strings
        print("Reading local CSV files...")
        fact_sales = spark.read.csv(
            'FactInternetSales.csv', header=True, inferSchema=True)
        dim_product = spark.read.csv(
            'DimProduct.csv', header=True, inferSchema=True)

        # 3. TRANSFORM: The 'Star Schema' Join
        # This combines the Sales (Fact) with Product Names (Dimension)
        print("Performing Inner Join on ProductKey...")
        joined_df = fact_sales.join(dim_product, on="ProductKey", how="inner")

        # 4. ENRICH: Calculate Profit (Business Logic)
        # .withColumn creates a new column based on a calculation
        print("Calculating Profit metrics...")
        enriched_df = joined_df.withColumn(
            "Profit",
            round(col("SalesAmount") - col("TotalProductCost"), 2)
        )

        # 5. SELECT: Clean the schema for Power BI
        # We only keep the columns that provide actual value to the business
        final_df = enriched_df.select(
            "SalesOrderNumber",
            "EnglishProductName",
            "Color",
            "SalesAmount",
            "Profit"
        )

        # 6. DISPLAY: Show the results in the terminal
        print("\nSample of the Enriched Data (Success!):")
        final_df.show(5)

        # 7. THE WINDOWS-FRIENDLY SAVE (Gold Layer)
        # To avoid the 'winutils.exe' error on Windows, we convert to Pandas for the final write
        print("Finalizing 'Gold' dataset...")
        final_pandas = final_df.toPandas()

        output_file = "curated_adventure_works.csv"
        final_pandas.to_csv(output_file, index=False)

        print(f"\nSUCCESS: Transformed data saved locally as: {output_file}")

    except Exception as e:
        print(f"ERROR: Transformation failed: {e}")
    finally:
        # Always stop the Spark session to free up your computer's RAM
        spark.stop()


if __name__ == "__main__":
    run_transformation()
