import os
import re
from pathlib import Path
import shutil

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, to_date, desc, regexp_replace, avg, count, row_number, lower, col

wd = os.getcwd()
data_file_name = "marketing_sample_for_careerbuilder_usa-careerbuilder_job_listing__20200401_20200630__30k_data.ldjson"
data_path = os.path.join(wd, data_file_name)
output_path = os.path.join(wd, "analysis_csv_files")


@udf
def html_filter_fn(s):
    return re.sub('<.*?>', '', s)


if __name__ == '__main__':
    # load data from kaggle
    if not os.path.exists(data_path):
        home = str(Path.home())
        wd = os.getcwd()

        shutil.copytree(os.path.join(wd, ".kaggle"), os.path.join(home, ".kaggle"))
        print("loading data from kaggle ...")
        import kaggle
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files('promptcloud/careerbuilder-job-listing-2020', path=wd, unzip=True)

    spark = SparkSession.builder.appName('jobs_analysis').getOrCreate()

    df = spark.read.json(data_path)

    # ------------------ calculate number of jobs posted on daily basis, per each city ---------------------------------
    jobsPerCityPerDay = df.groupBy(lower(col('city')).alias("city"), to_date("post_date", "yyy-MM-dd").alias("day")).count().repartition(1)

    # jobsPerCityPerDay.show(10, truncate=False)
    jobsPerCityPerDay.write.option("header", True).csv(os.path.join(output_path, "jobsPerCityPerDay"))

    # ------------------ calculate average salary per job title and state -------------------------------------
    avgSalaryPerjobTitlePerState = df.na.fill(value="0", subset=["inferred_salary_from", "inferred_salary_to"]). \
        select("job_title", "state", regexp_replace("inferred_salary_from", r'[^0-9\.,]+', "0").alias("sal_from"),
               regexp_replace("inferred_salary_to", r'[^0-9\.,]+', "0").alias("sal_to")). \
        select("job_title", "state", regexp_replace("sal_from", ',', '').alias("sal_from_c"),
               regexp_replace("sal_to", ',', '').alias("sal_to_c")). \
        selectExpr("job_title", "state", "cast(sal_from_c as double) sal_from_c",
                   "cast(sal_to_c as double) sal_to_c"). \
        selectExpr("job_title", "state", "(sal_from_c + sal_to_c)/2 mean_of_sal"). \
        groupBy("job_title", "state").agg(avg("mean_of_sal").alias("avg_salary")).repartition(1)

    # avgSalaryPerjobTitlePerState.printSchema()
    avgSalaryPerjobTitlePerState.write.option("header", True).csv(os.path.join(output_path, "avgSalaryPerjobTitlePerState"))

    # ----------------- identify the top 10 most active companies by number of positions opened -----------------------
    Top10Activedfcompanies = df.groupBy("company_name").agg(count("uniq_id").alias("# of positions opened")). \
        select("company_name", "# of positions opened", row_number().over(Window.partitionBy(). \
                                                                          orderBy(desc("# of positions opened")) ).alias("row_num")
               ).filter("row_num <= 10").select("company_name", "# of positions opened").repartition(1)

    # Top10Activedfcompanies.show()
    Top10Activedfcompanies.write.option("header", True).csv(os.path.join(output_path, "Top10Activedfcompanies"))

    # ---------------- create a UDF function to clean job description from HTML code contained inside -----------------
    cleanedDF = df.select(html_filter_fn("html_job_description")).alias("cleaned_job_description").repartition(1)

    # cleanedDF.show(10, truncate=False)
    cleanedDF.write.option("compression", "gzip").option("header", True).csv(os.path.join(output_path, "CleanedJobDescription"))

# ------------------ performance improved by using Built-in Spark SQL functions -----------------------------
    ImprovedCleanedDF = df.select(regexp_replace("html_job_description", r"<.*?>", "").alias("cleaned_job_description") )
    # ImprovedCleanedDF.show(10, truncate=False)