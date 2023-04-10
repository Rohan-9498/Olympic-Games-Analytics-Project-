-- Databricks notebook source
-- MAGIC %md # Let's discover more about the Olympic Games!
-- MAGIC This notebook contains various chart examples based on a sample Olympic Games dataset.  
-- MAGIC * Note, this dataset joins the athlete_events_final table and the noc_regions country codes table.
-- MAGIC * Notice that the country names do not match completely since the noc_regions country codes not compatiable to ISO 3166-1 alpha-3 standard.

-- COMMAND ----------

-- MAGIC %md ## Olympic Game by Geography 
-- MAGIC #### This is a world map of number of Olympic Games by country from a sample dataset

-- COMMAND ----------

-- DBTITLE 1,Olympic event (athlete-events) Data frame Definition
-- MAGIC %scala 
-- MAGIC 
-- MAGIC val athlete_events = sqlContext.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/FileStore/tables/athlete_events.csv")
-- MAGIC 
-- MAGIC display(athlete_events)

-- COMMAND ----------

-- DBTITLE 1,Printing Schema of Data frame
-- MAGIC %scala
-- MAGIC 
-- MAGIC athlete_events.printSchema

-- COMMAND ----------

-- DBTITLE 1,Modifying String to Double Datatype of Data frame
-- MAGIC %scala 
-- MAGIC import spark.implicits._;
-- MAGIC import org.apache.spark.sql.types._;
-- MAGIC import org.apache.spark.sql.functions;
-- MAGIC 
-- MAGIC val athlete_events_final = athlete_events.withColumn("ID",'ID.cast("Double"))
-- MAGIC                                          .withColumn("Age",'Age.cast("Double")) 
-- MAGIC                                          .withColumn("Height",'Height.cast("Double")) 
-- MAGIC                                          .withColumn("Weight",'Weight.cast("Double"))
-- MAGIC                                          .withColumn("Year",'Year.cast("Double")) 

-- COMMAND ----------

-- DBTITLE 1,Printing Schema after Modification
-- MAGIC %scala 
-- MAGIC athlete_events_final.printSchema

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC display(athlete_events_final)

-- COMMAND ----------

-- DBTITLE 1,Creating Temporary View from Data Frame
-- MAGIC %scala
-- MAGIC 
-- MAGIC athlete_events_final.createOrReplaceTempView("athlete_events_final")

-- COMMAND ----------

-- DBTITLE 1,NOC Regions (Country codes) Data frame Definition
-- MAGIC %scala 
-- MAGIC 
-- MAGIC val noc_regions = sqlContext.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/FileStore/tables/noc_regions.csv")
-- MAGIC 
-- MAGIC display(noc_regions)

-- COMMAND ----------

-- DBTITLE 1,Printing Schema of Data frame
-- MAGIC %scala
-- MAGIC 
-- MAGIC noc_regions.printSchema

-- COMMAND ----------

-- DBTITLE 1,Creating Temporary View from Data Frame
-- MAGIC %scala 
-- MAGIC 
-- MAGIC noc_regions.createOrReplaceTempView("noc_regions")

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC 
-- MAGIC select * from athlete_events_final;

-- COMMAND ----------

-- DBTITLE 1,Distribution of the age of gold medalists
-- MAGIC %sql
-- MAGIC 
-- MAGIC select count(Medal) as Medals, Age from athlete_events_final where Medal = 'Gold' group by Age order by Age asc;

-- COMMAND ----------

-- DBTITLE 1,Gold Medals for Athletes Over 50 based on Sports
-- MAGIC %sql
-- MAGIC 
-- MAGIC select Sport, Age from athlete_events_final where Medal = 'Gold' and Age >= 50; 

-- COMMAND ----------

-- DBTITLE 1,Women medals per edition(Summer Season) of the Games
-- MAGIC %sql 
-- MAGIC 
-- MAGIC select count(Medal) as Medals, Year from athlete_events_final where Sex = 'F' and Season = 'Summer' and Medal in ('Bronze','Gold','Silver') group by Year order by Year asc;

-- COMMAND ----------

-- DBTITLE 1,Top 5 Gold Medal Countries
-- MAGIC %sql 
-- MAGIC 
-- MAGIC select count(Medal) as Medals, region from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' group by region order by Medals desc limit 5; 

-- COMMAND ----------

-- DBTITLE 1,Disciplines with the greatest number of Gold Medals
-- MAGIC %sql 
-- MAGIC 
-- MAGIC select count(Medal) as Medals, Event from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' and A.NOC = 'USA' group by Event order by Medals desc; 

-- COMMAND ----------

-- DBTITLE 1,Height vs Weight of Olympic Medalists
-- MAGIC %sql
-- MAGIC 
-- MAGIC select Weight, Height from athlete_events_final where  Medal = 'Gold'; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Male Athletes over time
-- MAGIC %sql
-- MAGIC 
-- MAGIC select count(Sex) as Males, Year from athlete_events_final where Sex = 'M' and Season = 'Summer' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Female Athletes over time
-- MAGIC %sql 
-- MAGIC 
-- MAGIC select count(Sex) as Females, Year from athlete_events_final where Sex = 'F' and Season = 'Summer' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Age for Male Athletes over time
-- MAGIC %sql
-- MAGIC 
-- MAGIC select min(Age),mean(Age), max(Age), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Age for Female Athletes over time
select min(Age),mean(Age), max(Age), Year from athlete_events_final where Sex = 'F' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Weight for Male Athletes over time
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Weight for Female Athletes over time
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sex = 'F' and Year > 1925 group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Height for Male Athletes over time
select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Variation of Height for Female Athletes over time
select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sex = 'F' group by Year order by Year asc; 

-- COMMAND ----------

-- DBTITLE 1,Weight over year for Male Gymnasts
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Gymnastics' and Sex = 'M' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Weight over year for Female Gymnasts
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Gymnastics' and Sex = 'F' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Weight over years for Male Lifters
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'M' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Weight over year for Female Lifters
select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'F' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Height over year for Male Lifters
select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'M' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Height over year for Female Lifters
select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'F' and Year > 1950 group by Year order by Year;

-- COMMAND ----------

-- DBTITLE 1,Gold Medals based on Countries
select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' group by N.NOC

-- COMMAND ----------

-- DBTITLE 1,Silver Medals based on Countries
select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Silver' group by N.NOC

-- COMMAND ----------

-- DBTITLE 1,Bronze Medals based on Countries
select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Bronze' group by N.NOC

-- COMMAND ----------

select count(Medal) as Medals, case 
				WHEN N.NOC='ALG' THEN 'DZA'
				WHEN N.NOC='ANZ' THEN 'AUS'
				WHEN N.NOC='BAH' THEN 'BHS'
                WHEN N.NOC='BUL' THEN 'BGR'
				WHEN N.NOC='CRC' THEN 'CRI'
				WHEN N.NOC='CRO' THEN 'HRV'
                WHEN N.NOC='DEN' THEN 'DNK'
				WHEN N.NOC='EUN' THEN 'RUS'
				WHEN N.NOC='FIJ' THEN 'FJI'
                WHEN N.NOC='FRG' THEN 'DEU'
				WHEN N.NOC='GDR' THEN 'DEU'
				WHEN N.NOC='GER' THEN 'DEU'
                WHEN N.NOC='GRN' THEN 'GRD'
				WHEN N.NOC='IRI' THEN 'IRN'
				WHEN N.NOC='MGL' THEN 'MNG'
                WHEN N.NOC='NED' THEN 'NLD'
				WHEN N.NOC='NEP' THEN 'NPL'
				WHEN N.NOC='NGR' THEN 'NGA'
                WHEN N.NOC='POR' THEN 'PRT'
				WHEN N.NOC='PUR' THEN 'PRI'
				WHEN N.NOC='RSA' THEN 'ZAF'
                WHEN N.NOC='SCG' THEN 'SRB'
				WHEN N.NOC='SLO' THEN 'SVN'
				WHEN N.NOC='SUI' THEN 'CHE'
                WHEN N.NOC='TCH' THEN 'CZE'
				WHEN N.NOC='TPE' THEN 'TWN'
				WHEN N.NOC='UAE' THEN 'ARE'
                WHEN N.NOC='URS' THEN 'RUS'
				WHEN N.NOC='URU' THEN 'URY'
				WHEN N.NOC='VIE' THEN 'VNM'
                WHEN N.NOC='YUG' THEN 'SRB'
				WHEN N.NOC='ZIM' THEN 'ZWE'
                WHEN N.NOC='CHI' THEN 'CHL'
                WHEN N.NOC='GRE' THEN 'GRC'
				WHEN N.NOC='HAI' THEN 'HTI'
                WHEN N.NOC='INA' THEN 'IDN'
                WHEN N.NOC='LAT' THEN 'LVA'
				ELSE N.NOC
				END as COUNTRY
from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Gold' group by COUNTRY order by Medals desc;

-- COMMAND ----------

select count(Medal) as Medals, case 
				WHEN N.NOC='ALG' THEN 'DZA'
				WHEN N.NOC='ANZ' THEN 'AUS'
				WHEN N.NOC='BAH' THEN 'BHS'
                WHEN N.NOC='BUL' THEN 'BGR'
				WHEN N.NOC='CRC' THEN 'CRI'
				WHEN N.NOC='CRO' THEN 'HRV'
                WHEN N.NOC='DEN' THEN 'DNK'
				WHEN N.NOC='EUN' THEN 'RUS'
				WHEN N.NOC='FIJ' THEN 'FJI'
                WHEN N.NOC='FRG' THEN 'DEU'
				WHEN N.NOC='GDR' THEN 'DEU'
				WHEN N.NOC='GER' THEN 'DEU'
                WHEN N.NOC='GRN' THEN 'GRD'
				WHEN N.NOC='IRI' THEN 'IRN'
				WHEN N.NOC='MGL' THEN 'MNG'
                WHEN N.NOC='NED' THEN 'NLD'
				WHEN N.NOC='NEP' THEN 'NPL'
				WHEN N.NOC='NGR' THEN 'NGA'
                WHEN N.NOC='POR' THEN 'PRT'
				WHEN N.NOC='PUR' THEN 'PRI'
				WHEN N.NOC='RSA' THEN 'ZAF'
                WHEN N.NOC='SCG' THEN 'SRB'
				WHEN N.NOC='SLO' THEN 'SVN'
				WHEN N.NOC='SUI' THEN 'CHE'
                WHEN N.NOC='TCH' THEN 'CZE'
				WHEN N.NOC='TPE' THEN 'TWN'
				WHEN N.NOC='UAE' THEN 'ARE'
                WHEN N.NOC='URS' THEN 'RUS'
				WHEN N.NOC='URU' THEN 'URY'
				WHEN N.NOC='VIE' THEN 'VNM'
                WHEN N.NOC='YUG' THEN 'SRB'
				WHEN N.NOC='ZIM' THEN 'ZWE'
                WHEN N.NOC='CHI' THEN 'CHL'
                WHEN N.NOC='GRE' THEN 'GRC'
				WHEN N.NOC='HAI' THEN 'HTI'
                WHEN N.NOC='INA' THEN 'IDN'
                WHEN N.NOC='LAT' THEN 'LVA'
                WHEN N.NOC='AHO' THEN 'CUW'
				WHEN N.NOC='BOH' THEN 'CZE'
				WHEN N.NOC='BOT' THEN 'BWA'
                WHEN N.NOC='GUA' THEN 'GTM'
				WHEN N.NOC='ISV' THEN 'VGB'
				WHEN N.NOC='KSA' THEN 'SAU'
                WHEN N.NOC='LIB' THEN 'LBN'
				WHEN N.NOC='MAS' THEN 'MYS'
				WHEN N.NOC='NIG' THEN 'NER'
                WHEN N.NOC='PAR' THEN 'PRY'
				WHEN N.NOC='PHI' THEN 'PHL'
                WHEN N.NOC='SRI' THEN 'LKA'
                WHEN N.NOC='SUD' THEN 'SSD'
				WHEN N.NOC='TAN' THEN 'TZA'
                WHEN N.NOC='TGA' THEN 'TON'
                WHEN N.NOC='UAR' THEN 'SYR'
                WHEN N.NOC='ZAM' THEN 'ZMB'
				ELSE N.NOC
				END as COUNTRY
from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Silver' group by COUNTRY order by Medals desc;

-- COMMAND ----------

select count(Medal) as Medals, case 
				WHEN N.NOC='ALG' THEN 'DZA'
				WHEN N.NOC='ANZ' THEN 'AUS'
				WHEN N.NOC='BAH' THEN 'BHS'
                WHEN N.NOC='BUL' THEN 'BGR'
				WHEN N.NOC='CRC' THEN 'CRI'
				WHEN N.NOC='CRO' THEN 'HRV'
                WHEN N.NOC='DEN' THEN 'DNK'
				WHEN N.NOC='EUN' THEN 'RUS'
				WHEN N.NOC='FIJ' THEN 'FJI'
                WHEN N.NOC='FRG' THEN 'DEU'
				WHEN N.NOC='GDR' THEN 'DEU'
				WHEN N.NOC='GER' THEN 'DEU'
                WHEN N.NOC='GRN' THEN 'GRD'
				WHEN N.NOC='IRI' THEN 'IRN'
				WHEN N.NOC='MGL' THEN 'MNG'
                WHEN N.NOC='NED' THEN 'NLD'
				WHEN N.NOC='NEP' THEN 'NPL'
				WHEN N.NOC='NGR' THEN 'NGA'
                WHEN N.NOC='POR' THEN 'PRT'
				WHEN N.NOC='PUR' THEN 'PRI'
				WHEN N.NOC='RSA' THEN 'ZAF'
                WHEN N.NOC='SCG' THEN 'SRB'
				WHEN N.NOC='SLO' THEN 'SVN'
				WHEN N.NOC='SUI' THEN 'CHE'
                WHEN N.NOC='TCH' THEN 'CZE'
				WHEN N.NOC='TPE' THEN 'TWN'
				WHEN N.NOC='UAE' THEN 'ARE'
                WHEN N.NOC='URS' THEN 'RUS'
				WHEN N.NOC='URU' THEN 'URY'
				WHEN N.NOC='VIE' THEN 'VNM'
                WHEN N.NOC='YUG' THEN 'SRB'
				WHEN N.NOC='ZIM' THEN 'ZWE'
                WHEN N.NOC='CHI' THEN 'CHL'
                WHEN N.NOC='GRE' THEN 'GRC'
				WHEN N.NOC='HAI' THEN 'HTI'
                WHEN N.NOC='INA' THEN 'IDN'
                WHEN N.NOC='LAT' THEN 'LVA'
                WHEN N.NOC='AHO' THEN 'CUW'
				WHEN N.NOC='BOH' THEN 'CZE'
				WHEN N.NOC='BOT' THEN 'BWA'
                WHEN N.NOC='GUA' THEN 'GTM'
				WHEN N.NOC='ISV' THEN 'VGB'
				WHEN N.NOC='KSA' THEN 'SAU'
                WHEN N.NOC='LIB' THEN 'LBN'
				WHEN N.NOC='MAS' THEN 'MYS'
				WHEN N.NOC='NIG' THEN 'NER'
                WHEN N.NOC='PAR' THEN 'PRY'
				WHEN N.NOC='PHI' THEN 'PHL'
                WHEN N.NOC='SRI' THEN 'LKA'
                WHEN N.NOC='SUD' THEN 'SSD'
				WHEN N.NOC='TAN' THEN 'TZA'
                WHEN N.NOC='TGA' THEN 'TON'
                WHEN N.NOC='UAR' THEN 'SYR'
                WHEN N.NOC='ZAM' THEN 'ZMB'
				ELSE N.NOC
				END as COUNTRY
from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Bronze' group by COUNTRY order by Medals desc limit 75;
