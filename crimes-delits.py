import sys
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark import SparkContext

#cretion du spark context
sc = SparkContext()
hiveContext = HiveContext(sc)

#param fichier, separateur
result_crime_delit_file = sc.textFile(sys.argv[1])
header_crime_delit_ = result_crime_delit_file.first()
result_crime_delit_file_no_header = result_crime_delit_file.filter(lambda line_e: line_e != header_crime_delit_)
parts = result_crime_delit_file_no_header.map(lambda l: l.split(";"))


# SQL selection des colonnes
result = parts.map(lambda p: Row(codeCommune=p[0], annee=p[1], nbFaits=p[5])
)

#creation dataframe
dfresultat = hiveContext.createDataFrame(result)

#cretion de la table
dfresultat.registerTempTable("resultat")

#requete
resultat = hiveContext.sql("SELECT codeCommune, annee,"
  " SUM(nbFaits) as nbFaits"
  " FROM resultat"
  " WHERE annee = 21 AND nbFaits > 0"
  " GROUP BY codeCommune, annee"
  " ORDER BY nbFaits DESC"
  " LIMIT 100"
  ).show(dfresultat.count(), False)