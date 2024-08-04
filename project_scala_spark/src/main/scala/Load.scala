package scala
import org.apache.spark.sql.{DataFrame, SaveMode}

object Load {
  /**
   * Cette fonction permet de souvegarder les données dans un repertoire local
   *
   * @param df       : Dataframe
   * @param saveMode : methode de souvegarde
   * @param format   : format des fichiers de sauvegarde
   * @param path     : chemin où les données doivent etre sauvegardées
   * @return : Nothing
   */
  /*
   Ecrire une fontion de sauvegarder les données d'un dataframe spark
   Completer la fonction ci-dessous.
    */
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
