import sys
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark import SparkContext


#cretion du spark context
sc = SparkContext()
hiveContext = HiveContext(sc)


#param fichier, separateur
#election
result_election_file = sc.textFile(sys.argv[1])
header_election = result_election_file.first()
result_election_file_no_header = result_election_file.filter(lambda line_e: line_e != header_election)
parts = result_election_file_no_header.map(lambda l: l.split(";"))

#chommages
result_chomage_file = sc.textFile(sys.argv[2])
header_chomage = result_chomage_file.first()
result_chomage_file_no_header = result_chomage_file.filter(lambda line_e: line_e != header_chomage)
parts = result_chomage_file_no_header.map(lambda l: l.split(";"))

#salaires
result_salaire_file = sc.textFile(sys.argv[3])
header_salaire = result_salaire_file.first()
result_salaire_file_no_header = result_salaire_file.filter(lambda line_e: line_e != header_salaire)
parts = result_salaire_file_no_header.map(lambda l: l.split(";"))

#departement lie a salaire
result_departement_file = sc.textFile(sys.argv[4])
header_departement = result_departement_file.first()
result_departement_file_no_header = result_departement_file.filter(lambda line_e: line_e != header_departement)
parts2 = result_departement_file_no_header.map(lambda l: l.split(";"))


# SQL selection des colonnes
election = parts.map(lambda p: Row(nomDepartement=p[1],
    PctVoteExpC1=p[27], PctVoteExpC2=p[34], PctVoteExpC3=p[41], PctVoteExpC4=p[48],
    PctVoteExpC5=p[55], PctVoteExpC6=p[62], PctVoteExpC7=p[69], PctVoteExpC8=p[76],
    PctVoteExpC9=p[83], PctVoteExpC10=p[90], PctVoteExpC11=p[97], PctVoteExpC12=p[104])
)

chommage = parts.map(lambda p: Row(nomDepartement=p[1],
    Chomage2021T1=p[158], Chomage2021T2=p[159], Chomage2021T3=p[160], Chomage2021T4=p[161])
)

salaire = parts.map(lambda p: Row(codeDepartement=p[0],
    moyenneNetHor2018=p[1], moyenneNetHorCadreSup2018=p[2], moyenneNetHorProfInter2018=p[3], moyenneNetHorEmployees2018=p[4],
    moyenneNetHorOuvrier2018=p[5], moyenneNet18a25Ans2018=p[16], moyenneNet26a50Ans2018=p[17], moyenneNetPlus25Ans2018=p[18])
)

departement = parts2.map(lambda p: Row(codeDepartement=p[2], nomDep=p[3])
)


#creation dataframe
dfelection = hiveContext.createDataFrame(election)
dfchommage = hiveContext.createDataFrame(chommage)
dfsalaire = hiveContext.createDataFrame(salaire)
dfdepartement = hiveContext.createDataFrame(departement)


#cretion de la table
dfelection.registerTempTable("election")
dfchommage.registerTempTable("chommage")
dfsalaire.registerTempTable("salaire")
dfdepartement.registerTempTable("departement")


#requete
election = hiveContext.sql("SELECT election.nomDepartement,"
  " ROUND(AVG(election.PctVoteExpC1), 2) as ARTHAUD,"
  " ROUND(AVG(election.PctVoteExpC2), 2) as ROUSSEL,"
  " (chommage.Chomage2021T1 + chommage.Chomage2021T2 + chommage.Chomage2021T3 + chommage.Chomage2021T4) / 4 as moyenChomage2021"
  " FROM election"
  " INNER JOIN chommage ON election.nomDepartement = chommage.nomDepartement"
  " GROUP BY nomDepartement"
  " ORDER BY nomDepartement"
  ).show(dfresultat.count(), False)