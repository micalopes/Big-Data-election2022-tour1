import sys
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark import SparkContext

#cretion du spark context
sc = SparkContext()
hiveContext = HiveContext(sc)

#param fichier, separateur
result_chomage_file = sc.textFile(sys.argv[1])
header_chomage = result_chomage_file.first()
result_chomage_file_no_header = result_chomage_file.filter(lambda line_e: line_e != header_chomage)
parts = result_chomage_file_no_header.map(lambda l: l.split(";"))


# SQL selection des colonnes
result = parts.map(lambda p: Row(departement=p[1],
    Chomage2021T1=p[158], Chomage2021T2=p[159], Chomage2021T3=p[160], Chomage2021T4=p[161])
)

#creation dataframe
dfresultat = hiveContext.createDataFrame(result)

#cretion de la table
dfresultat.registerTempTable("resultat")

#requete
resultat = hiveContext.sql("SELECT departement, Chomage2021T1, Chomage2021T2, Chomage2021T3, Chomage2021T4"
  " FROM resultat"
  " ORDER BY departement"
  ).show(dfresultat.count(), False)