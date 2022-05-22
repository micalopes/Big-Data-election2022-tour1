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
    PctVoteExpC1=p[41], PctVoteExpC2=p[55], PctVoteExpC3=p[62], PctVoteExpC4=p[69],
    PctVoteExpC5=p[83], PctVoteExpC6=p[90])
)

chomage = parts2.map(lambda p: Row(ChomageCodeDepartement=p[0], ChomageNomDepartement=p[1],
    Chomage2021T1=p[158], Chomage2021T2=p[159], Chomage2021T3=p[160], Chomage2021T4=p[161])
)

salaire = parts3.map(lambda p: Row(SalaireCodeDepartement=p[0], SalaireMoyenNetHor2018=p[1],
    SalaireMoyenNetHorCadreSup2018=p[2], SalaireMoyenNetHorProfInter2018=p[3], SalaireMoyenNetHorEmployees2018=p[4],
    SalaireMoyenNetHorOuvrier2018=p[5], SalaireMoyenNetHor18a25Ans2018=p[16], SalaireMoyenNetHor26a50Ans2018=p[17],
    SalaireMoyenNetHorPlus25Ans2018=p[18])
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
electionInfluence = hiveContext.sql("SELECT ElecNomDepartement as Departement,"
    " ROUND(AVG(PctVoteExpC1), 2) as Macron,"
    " ROUND(AVG(PctVoteExpC2), 2) as Lepen,"
    " ROUND(AVG(PctVoteExpC3), 2) as Zemmour,"
    " ROUND(AVG(PctVoteExpC4), 2) as Melenchon,"
    " ROUND(AVG(PctVoteExpC5), 2) as Jadot,"
    " ROUND(AVG(PctVoteExpC6), 2) as Pecresse,"
    " ROUND(((Chomage2021T1 + Chomage2021T2 + Chomage2021T3 + Chomage2021T4) / 4), 2) as MoyenneChomage2021,"
    " ROUND(SalaireMoyenNetHor2018, 2) as SHorMoy,"
    " ROUND(SalaireMoyenNetHorCadreSup2018, 2) as SHorMoyCadreSup,"
    " ROUND(SalaireMoyenNetHorProfInter2018, 2) as SalaireHorMoyProfInter,"
    " ROUND(SalaireMoyenNetHorEmployees2018, 2) as SHorMoyEmploye,"
    " ROUND(SalaireMoyenNetHorOuvrier2018, 2) as SHorMoyOuvrier,"
    " ROUND(SalaireMoyenNetHor18a25Ans2018, 2) as SHorMoy18a25Ans,"
    " ROUND(SalaireMoyenNetHor26a50Ans2018, 2) as SHorMoy26a50Ans,"
    " ROUND(SalaireMoyenNetHorPlus25Ans2018, 2) as SHorMoyPlus25Ans"
    " FROM electionInfluence"
    " WHERE ElecCodeDepartement = 59 OR ElecCodeDepartement = 75 OR ElecCodeDepartement = 13 OR ElecCodeDepartement = 69"
        " OR ElecCodeDepartement = 92 OR ElecCodeDepartement = 93 OR ElecCodeDepartement = 33 OR ElecCodeDepartement = 62"
        " OR ElecCodeDepartement = 78 OR ElecCodeDepartement = 77 OR ElecCodeDepartement = 94 OR ElecCodeDepartement = 44"
        " OR ElecCodeDepartement = 31 OR ElecCodeDepartement = 91 OR ElecCodeDepartement = 76"
    " GROUP BY ElecNomDepartement, Chomage2021T1, Chomage2021T2, Chomage2021T3, Chomage2021T4, SalaireMoyenNetHor2018,"
        " SalaireMoyenNetHorCadreSup2018, SalaireMoyenNetHorProfInter2018, SalaireMoyenNetHorEmployees2018, SalaireMoyenNetHorOuvrier2018,"
        " SalaireMoyenNetHor18a25Ans2018, SalaireMoyenNetHor26a50Ans2018, SalaireMoyenNetHorPlus25Ans2018"
    " ORDER BY MoyenneChomage2021 DESC"
).show(dfResult.count(), False)