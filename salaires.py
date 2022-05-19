import sys
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark import SparkContext

#cretion du spark context
sc = SparkContext()
hiveContext = HiveContext(sc)

#param fichier, separateur
result_salaire_file = sc.textFile(sys.argv[1])
header_salaire = result_salaire_file.first()
result_salaire_file_no_header = result_salaire_file.filter(lambda line_e: line_e != header_salaire)
parts = result_salaire_file_no_header.map(lambda l: l.split(";"))

result_departement_file = sc.textFile(sys.argv[2])
header_departement = result_departement_file.first()
result_departement_file_no_header = result_departement_file.filter(lambda line_e: line_e != header_departement)
parts2 = result_departement_file_no_header.map(lambda l: l.split(";"))



# SQL selection des colonnes
result = parts.map(lambda p: Row(codeDepartement=p[0],
    moyenneNetHor2018=p[1], moyenneNetHorCadreSup2018=p[2], moyenneNetHorProfInter2018=p[3], moyenneNetHorEmployees2018=p[4],
    moyenneNetHorOuvrier2018=p[5], moyenneNet18a25Ans2018=p[16], moyenneNet26a50Ans2018=p[17], moyenneNetPlus25Ans2018=p[18])
)

departement = parts2.map(lambda p: Row(codeDepartement=p[2], nomDep=p[3])
)


#creation dataframe
dfresultat = hiveContext.createDataFrame(result)
dfdepartement = hiveContext.createDataFrame(departement)


#cretion de la table
dfresultat.registerTempTable("resultat")
dfdepartement.registerTempTable("departement")


#requete
resultat = hiveContext.sql("SELECT departement.nomDep,"
  " ROUND(resultat.moyenneNetHor2018, 2) as NetHor,"
  " ROUND(resultat.moyenneNetHorCadreSup2018, 2) as NetHorCadreSup,"
  " ROUND(resultat.moyenneNetHorProfInter2018, 2) as NetHorProfInter,"
  " ROUND(resultat.moyenneNetHorEmployees2018, 2) as NetHorEmploye,"
  " ROUND(resultat.moyenneNetHorOuvrier2018, 2) as NetHorOuvrier,"
  " ROUND(resultat.moyenneNet18a25Ans2018, 2) as Net18a25Ans,"
  " ROUND(resultat.moyenneNet26a50Ans2018, 2) as Net26a50Ans,"
  " ROUND(resultat.moyenneNetPlus25Ans2018, 2) as NetPlus25Ans"
  " FROM resultat"
  " INNER JOIN departement ON resultat.codeDepartement = departement.codeDepartement"
  " ORDER BY departement.nomDep"
  ).show(dfresultat.count(), False)