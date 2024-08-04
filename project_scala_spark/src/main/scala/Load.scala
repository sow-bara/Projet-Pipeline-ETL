package scala
import org.apache.spark.sql.{DataFrame, SaveMode}

object Load {

  def saveData(df: DataFrame, saveMode: String, format: String, path: String): Unit = {
    try {
      df.write
        .format(format)
        .mode(SaveMode.valueOf(saveMode.capitalize))
        .save(path)
    } catch {
      case e: Exception => {
        Utils._log.error(s"Error saving data: ${e.getMessage}")
        throw e
      }
    }
  }
}
