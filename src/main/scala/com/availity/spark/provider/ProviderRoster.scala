package com.availity.spark.provider

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, concat, count, lit, month}

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    process
  }

  def process(): Unit = {
    //create spark session.
    val spark = SparkSession.builder().appName("ProviderRoster").master("local[*]").getOrCreate()

    //load the providers csv file.
    val providersDF = spark.read.format("csv")
      .option("header","true")
      .option("delimiter", "|")
      .load("data/providers.csv")

    //load the visits csv file.
    val visitsDF = spark.read.format("csv").load("data/visits.csv")
      //rename columns as there is no header line on the csv file.
      .withColumnRenamed("_c0", "visit_id")
      .withColumnRenamed("_c1", "provider_id")
      .withColumnRenamed("_c2", "date_of_service")
      //convert date of serice to DateType.
      .withColumn("date_of_service", col("date_of_service").cast(DateType))
      //Add new column to contain the month of service (month number).
      .withColumn("month_of_service", month(col("date_of_service")))

    //1. Given the two data datasets, calculate the total number of visits per provider.
    // The resulting set should contain the provider's ID, name, specialty,
    // along with the number of visits.
    // Output the report in json, partitioned by the provider's specialty.

    //Get count of visits per provider Id.
    val numOfVisitsDF = visitsDF.groupBy("provider_id").count().withColumnRenamed("count","number_of_visits")
    // Join count of visits on providers to prepare final Dataframe.
    val resultDF = numOfVisitsDF.join(
      // Add column to contain Last, Middle and First Name of Provider.
      providersDF.withColumn("provider_name", concat(col("last_name"), lit(" "), col("middle_name"), lit(" "), col("first_name"))), "provider_id", "inner")
                  .orderBy(col("number_of_visits").desc)
                  .select("provider_id", "provider_name", "provider_specialty", "number_of_visits")

    //write result 1 to json file.
    resultDF.write.mode(SaveMode.Overwrite).partitionBy("provider_specialty").json("data/output/visits_per_provider.json")

    //2. Given the two datasets, calculate the total number of visits per provider per month.
    // The resulting set should contain the provider's ID, the month, and total number of visits. Output the result set in json.

    //Get count of visit per provider and month number.
    val resultDF2 = visitsDF.groupBy("provider_id", "month_of_service").count().withColumnRenamed("count","number_of_visits")
                            .orderBy(col("provider_id").desc, col("month_of_service").asc)
                            .select("provider_id", "month_of_service", "number_of_visits")

    //write result 2 to json file
    resultDF2.write.mode(SaveMode.Overwrite).json("data/output/visits_per_provider_month.json")
  }

}