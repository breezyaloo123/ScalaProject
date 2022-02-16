// Databricks notebook source
val spark =SparkSession
.builder()
.appName("PROJETBIGDATA")
.config("spark.some.config.option","some-value")
.getOrCreate()

// COMMAND ----------

// MAGIC %fs ls FileStore/tables/PROJET

// COMMAND ----------

val toursDF = spark.read.option("header","true").format("csv").load("/FileStore/tables/PROJET/tours-1.csv")

// COMMAND ----------

display(toursDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//Convertir les valeurs de la colonne company_permalink to lowercase pour le dataframe toursDF
val toursDF1 = toursDF.withColumn("company_permalink",lower(col("company_permalink")))

// COMMAND ----------

display(toursDF1)

// COMMAND ----------

val societeDF = spark.read.option("header","true").format("csv").load("/FileStore/tables/PROJET/societe1-5.csv")

// COMMAND ----------

display(societeDF)

// COMMAND ----------

societeDF.show

// COMMAND ----------

//Convertir les valeurs de la colonne permalink to lowercase pour le dataframe societeDF

val societeDF1 = societeDF.withColumn("permalink",lower(col("permalink")))

// COMMAND ----------

display(societeDF1)

// COMMAND ----------

toursDF.count

// COMMAND ----------

toursDF.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//quel est le nombre de sociétés différentes présentes dans	le dataframe toursDf?
toursDF.select("company_permalink").agg(countDistinct('company_permalink) as 'DistinctCompanies).show()

// COMMAND ----------

val societeDF1 = societeDF.withColumn("permalink",lower(col("permalink")))

// COMMAND ----------


societeDF1.show()

// COMMAND ----------

societeDF1.count

// COMMAND ----------

societeDF1.take(100)

// COMMAND ----------

display(societeDF1)

// COMMAND ----------

import org.apache.spark.sql.types._
import java.sql.Date


// COMMAND ----------

//quel est le nombre de sociétés différentes présentes dans	le dataframe societesDf?
societeDF1.select("permalink").agg(countDistinct($"permalink").alias("DistinctCompanies")).show()

// COMMAND ----------

societeDF1.count

// COMMAND ----------

societeDF1.select("permalink")
 .where(col("permalink").isNotNull)
 .agg(countDistinct('permalink) as 'DistinctCompanies)
 .show()


// COMMAND ----------

//dans le	dataframe	societesDf,	quelle colonne	peut être utilisé	comme clé	unique pour chaque société.Donnez le nom
//de la colonne

Le nom de cette colonne est permalink

// COMMAND ----------

//Il existe bel et bien des sociétés qui se trouvent dans societeDF et qui sont absentes dans toursDF
//Voici ci-dessous le nombre
val diff_columns = toursDF1.join(societeDF1,toursDF1("company_permalink") === societeDF1("permalink"),"left").where(col("permalink").isNull)

// COMMAND ----------

diff_columns.count

// COMMAND ----------

toursDF1.select(lower(toursDF1.col("company_permalink"))).show

// COMMAND ----------

//fusionner	les	deux	dataframes afin	que	toutes	les	
//colonnes	du	dataframe	societesDf soient	ajoutées	au	dataframe	
//toursDf.	Nommez	le	nouveau	dataframe	obtenu mergedDf.	
//Combien	d'observations	sont	présentes	dans	mergedDf ?

val mergedDF = toursDF1.join(societeDF1,toursDF1("company_permalink") === societeDF1("permalink"),"left")

// COMMAND ----------

mergedDF.count

// COMMAND ----------

mergedDF.show(114949)

// COMMAND ----------

mergedDF.select("company_permalink").where(col("permalink").isNull).show

// COMMAND ----------

//calculez	la	valeur	la	plus représentative	du	montant de	
//l'investissement	pour	chaque	type	d’investissement	(colonne	
//type_tour_investissement du	datafarme	toursDf).
val valeur_investissement= toursDF.select("funding_round_type","raised_amount_usd")
 .where(col("funding_round_type").isNotNull)
 .groupBy("funding_round_type").agg(max("raised_amount_usd").alias("maxvalue"))
 .orderBy(desc("maxvalue"))

// COMMAND ----------

valeur_investissement.show()

// COMMAND ----------

display(valeur_investissement)

// COMMAND ----------

toursDF.select("funding_round_type","raised_amount_usd")
 .where(col("funding_round_type").isNotNull)
 .groupBy("funding_round_type")
  .agg(count("funding_round_type").alias("nombre_investissement"))
 .orderBy(desc("nombre_investissement"))
 .show(false)

// COMMAND ----------

//Sur	la	base	du	montant	d'investissement	le	plus	
//représentatif	calculé	ci-dessus,	quel	type	d'investissement	pensez-vous	être	le	plus	approprié	pour	LightInvest.
Le type d'investissement le plus approprié pour LightInvest est grant

// COMMAND ----------

//Étant	donné	que	LightInvest souhaite	investir	entre	4	
//et	16 millions	d’euros par	tour	d'investissement,	quel	type	
//d'investissement	leur	convient	le	mieux	?

le type d'investissement qui leur convient est grant


// COMMAND ----------

//quels	sont	les	sept premiers	pays	qui	ont	reçu	
//l’investissement	total	le	plus	élevé	(dans	tous	les	secteurs	pour	le	
//type d'investissement	choisi).
val paysDF = spark.read.option("header","true").format("csv").load("/FileStore/tables/PROJET/payss.csv")

// COMMAND ----------

display(paysDF)

// COMMAND ----------

val payscode = mergedDF.join(paysDF,societeDF1("country_code")===paysDF("code"),"left")

// COMMAND ----------

display(societeDF1.select($"country_code"))

// COMMAND ----------

val country_code =societeDF1.select($"country_code").where(col("country_code").isNotNull)

// COMMAND ----------

display(payscode.where(col("code").isNull))

// COMMAND ----------

display(payscode.where(col("funding_round_type")==="venture").where(col("country_code").isNull))

// COMMAND ----------

//	quels	sont	les	sept premiers	pays	qui	ont	reçu	
//l’investissement	total	le	plus	élevé	(dans	tous	les	secteurs	pour	le	
//type	d'investissement	choisi)
val res=payscode.select("funding_round_type","pays","country_code","raised_amount_usd")
 .where(col("funding_round_type")==="venture")
 .groupBy("funding_round_type","country_code")
  .agg(count("funding_round_type").alias("count"),sum("raised_amount_usd").alias("total"),avg($"raised_amount_usd").alias("moyenne"))
 .orderBy(desc("total")).limit(7)

// COMMAND ----------

display(res)

// COMMAND ----------

//	quels	sont	les	sept premiers	pays	qui	ont	reçu	
//l’investissement	total	le	plus	élevé	(dans	tous	les	secteurs	pour	le	
//type	d'investissement	choisi)
val ress=mergedDF.select("funding_round_type","country_code","raised_amount_usd")
 .where(col("funding_round_type")==="venture").where(col("country_code").isNotNull)
 .groupBy("funding_round_type","country_code")
  .agg(count("funding_round_type").alias("count"),sum("raised_amount_usd").alias("total"),avg($"raised_amount_usd").alias("moyenne"))
 .orderBy(desc("total")).limit(7)

// COMMAND ----------

display(ress)

// COMMAND ----------

//pour	le	type	d'investissement	choisi,	créez	un	
//dataframe	nommé	top7CountriesDf avec	les	sept premiers	pays	
//(en	fonction	du	montant	total	d'investissement	reçu	par	chaque	
//pays).

val top7CountriesDf=mergedDF.select("funding_round_type","country_code","raised_amount_usd")
 .where(col("funding_round_type")==="venture")
 .groupBy("country_code")
  .agg(count("funding_round_type").alias("nombre_investissement"),sum("raised_amount_usd").alias("montant"))
 .orderBy(desc("montant")).limit(7)

// COMMAND ----------

display(top7CountriesDf)

// COMMAND ----------

//identifiez	les	trois	premiers	pays	anglophones	dans	le	
//dataframe	top7CountriesDf.

USA , GBR , IND
