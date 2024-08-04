package scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Transform {

  def cleanData(df: DataFrame): DataFrame = {
    // 0. Convert timestamp columns to "YYYY/MM/DD HH:MM SSS" format
    val firstDF = df.withColumn("event_timestamp", from_unixtime(col("event_timestamp") / 1000, "yyyy/MM/dd HH:mm:ss"))
      .withColumn("event_previous_timestamp", from_unixtime(col("event_previous_timestamp") / 1000, "yyyy/MM/dd HH:mm:ss"))
      .withColumn("user_first_touch_timestamp", from_unixtime(col("user_first_touch_timestamp") / 1000, "yyyy/MM/dd HH:mm:ss"))

    // 1. Add revenue column from ecommerce.purchase_revenue_in_usd
    val revenueDF = firstDF.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))

    // 2. Filter events with non-null revenue
    val purchasesDF = revenueDF.filter(col("revenue").isNotNull)

    // 3. Find unique event_name values generating revenue
    val distinctDF = purchasesDF.select("event_name").distinct()

    // 4. Drop unnecessary columns
    val cleanDF = purchasesDF.drop("event_name")

    cleanDF
  }

  def computeTrafficRevenue(df: DataFrame): DataFrame = {
    // 5. Calculate total and average revenue by traffic source, state, and city
    val trafficDF = df.groupBy("traffic_source", "geo.state", "geo.city")
      .agg(
        sum("revenue").alias("total_rev"),
        avg("revenue").alias("avg_rev")
      )

    // 6. Get top five traffic sources by total revenue
    val windowSpec = Window.orderBy(col("total_rev").desc)
    val topTrafficDF = trafficDF.withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 5)
      .drop("rank")

    // 7. Limit revenue columns to two decimal places
    val finalDF = topTrafficDF.withColumn("total_rev", round(col("total_rev"), 2))
      .withColumn("avg_rev", round(col("avg_rev"), 2))

    finalDF
  }
}
