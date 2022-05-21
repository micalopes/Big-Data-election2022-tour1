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
parts1 = result_election_file_no_header.map(lambda l: l.split(";"))

#chomages
result_chomage_file = sc.textFile(sys.argv[2])
header_chomage = result_chomage_file.first()
result_chomage_file_no_header = result_chomage_file.filter(lambda line_e: line_e != header_chomage)
parts2 = result_chomage_file_no_header.map(lambda l: l.split(";"))

#salaires
result_salaire_file = sc.textFile(sys.argv[3])
header_salaire = result_salaire_file.first()
result_salaire_file_no_header = result_salaire_file.filter(lambda line_e: line_e != header_salaire)
parts3 = result_salaire_file_no_header.map(lambda l: l.split(";"))


# SQL selection des colonnes
election = parts1.map(lambda p: Row(ElecCodeDepartement=p[0], ElecNomDepartement=p[1],
    PctVoteExpC1=p[27], PctVoteExpC2=p[34], PctVoteExpC3=p[41], PctVoteExpC4=p[48],
    PctVoteExpC5=p[55], PctVoteExpC6=p[62], PctVoteExpC7=p[69], PctVoteExpC8=p[76],
    PctVoteExpC9=p[83], PctVoteExpC10=p[90], PctVoteExpC11=p[97], PctVoteExpC12=p[104])
)

chomage = parts2.map(lambda p: Row(ChomageCodeDepartement=p[0], ChomageNomDepartement=p[1],
    Chomage2021T1=p[158], Chomage2021T2=p[159], Chomage2021T3=p[160], Chomage2021T4=p[161])
)

salaire = parts3.map(lambda p: Row(SalaireCodeDepartement=p[0], SalaireMoyenNetHor2018=p[1])
)


#creation dataframe
dfElection = hiveContext.createDataFrame(election)
dfChomage = hiveContext.createDataFrame(chomage)
dfSalaire = hiveContext.createDataFrame(salaire)

#jointure
dfPreResult = dfElection.join(dfChomage, dfElection.ElecCodeDepartement == dfChomage.ChomageCodeDepartement, "inner")
dfResult = dfPreResult.join(dfSalaire, dfPreResult.ElecCodeDepartement == dfSalaire.SalaireCodeDepartement, "inner")


#cretion de la table
dfResult.registerTempTable("electionInfluence")


#requete
#requete
electionInfluence = hiveContext.sql("SELECT ElecNomDepartement as Departement,"
    " ROUND(AVG(PctVoteExpC1), 2) as Arthaud,"
    " ROUND(AVG(PctVoteExpC2), 2) as Roussel,"
    " ROUND(AVG(PctVoteExpC3), 2) as Macron,"
    " ROUND(AVG(PctVoteExpC4), 2) as Lassalle,"
    " ROUND(AVG(PctVoteExpC5), 2) as Lepen,"
    " ROUND(AVG(PctVoteExpC6), 2) as Zemmour,"
    " ROUND(AVG(PctVoteExpC7), 2) as Melenchon,"
    " ROUND(AVG(PctVoteExpC8), 2) as Hidalgo,"
    " ROUND(AVG(PctVoteExpC9), 2) as Jadot,"
    " ROUND(AVG(PctVoteExpC10), 2) as Pecresse,"
    " ROUND(AVG(PctVoteExpC11), 2) as Poutou,"
    " ROUND(AVG(PctVoteExpC12), 2) as DupontAignan,"
    " ROUND(((Chomage2021T1 + Chomage2021T2 + Chomage2021T3 + Chomage2021T4) / 4), 2) as MoyenneChomage2021,"
    " ROUND(SalaireMoyenNetHor2018, 2) as SalaireHoraireMoyen"
    " FROM electionInfluence"
    " WHERE ElecCodeDepartement = 59 OR ElecCodeDepartement = 75 OR ElecCodeDepartement = 13 OR ElecCodeDepartement = 69"
        " OR ElecCodeDepartement = 92 OR ElecCodeDepartement = 93 OR ElecCodeDepartement = 33 OR ElecCodeDepartement = 62"
        " OR ElecCodeDepartement = 78 OR ElecCodeDepartement = 77"
    " GROUP BY ElecNomDepartement, Chomage2021T1, Chomage2021T2, Chomage2021T3, Chomage2021T4, SalaireMoyenNetHor2018"
    " ORDER BY ElecNomDepartement"
).show(dfResult.count(), False)