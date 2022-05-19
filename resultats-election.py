import sys
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark import SparkContext

#cretion du spark context
sc = SparkContext()
hiveContext = HiveContext(sc)

#param fichier, separateur
result_election_file = sc.textFile(sys.argv[1])
header_election = result_election_file.first()
result_election_file_no_header = result_election_file.filter(lambda line_e: line_e != header_election)
parts = result_election_file_no_header.map(lambda l: l.split(";"))

# SQL selection des colonnes
result = parts.map(lambda p: Row(departement=p[1],
    PctVoteExpC1=p[27], PctVoteExpC2=p[34], PctVoteExpC3=p[41], PctVoteExpC4=p[48],
    PctVoteExpC5=p[55], PctVoteExpC6=p[62], PctVoteExpC7=p[69], PctVoteExpC8=p[76],
    PctVoteExpC9=p[83], PctVoteExpC10=p[90], PctVoteExpC11=p[97], PctVoteExpC12=p[104])
)

#creation dataframe
dfresultat = hiveContext.createDataFrame(result)

#cretion de la table
dfresultat.registerTempTable("resultat")

#requete
resultat = hiveContext.sql("SELECT departement,"
  " ROUND(AVG(PctVoteExpC1), 2) as ARTHAUD,"
  " ROUND(AVG(PctVoteExpC2), 2) as ROUSSEL,"
  " ROUND(AVG(PctVoteExpC3), 2) as MACRON,"
  " ROUND(AVG(PctVoteExpC4), 2) as LASSALLE,"
  " ROUND(AVG(PctVoteExpC5), 2) as LEPEN,"
  " ROUND(AVG(PctVoteExpC6), 2) as ZEMMOUR,"
  " ROUND(AVG(PctVoteExpC7), 2) as MELENCHON,"
  " ROUND(AVG(PctVoteExpC8), 2) as HIDALGO,"
  " ROUND(AVG(PctVoteExpC9), 2) as JADOT,"
  " ROUND(AVG(PctVoteExpC10), 2) as PECRESSE,"
  " ROUND(AVG(PctVoteExpC11), 2) as POUTOU,"
  " ROUND(AVG(PctVoteExpC12), 2) as DUPONTAIGNAN"
  " FROM resultat"
  " GROUP BY departement"
  " ORDER BY departement"
  ).show(dfresultat.count(), False)