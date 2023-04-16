import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    appName("ENBD"). \
    getOrCreate()

### Read data

df = spark.read.csv("/dataset/nyc-jobs.csv", header=True)
df.printSchema()

# KPIs

## Top 10 Job Postings per Category

df = df.withColumnRenamed("Job Category","jobCategory")\
    .withColumnRenamed("Salary Range From","salaryRangeFrom")\
    .withColumnRenamed("Salary Range To","salaryRangeTo")\
    .withColumnRenamed("Business Title","businessTitle")\
    .withColumnRenamed("Posting Date","postingDate")\
    .withColumnRenamed("Job ID","jobId")\
    .withColumnRenamed("Salary Frequency","salaryFrequency")


from pyspark.sql.functions import *
import matplotlib.pyplot as plt

# Create a pandas dataframe from the grouped data
top_jobs = df.groupBy("jobCategory").agg(count("*").alias("c")).orderBy(col("c").desc()).limit(10).toPandas()

# Create a bar chart using Matplotlib
fig, ax = plt.subplots(figsize=(8,6))
ax.bar(top_jobs['jobCategory'], top_jobs['c'])

# Set chart title and axis labels
ax.set_title('Top 10 Job Categories')
ax.set_xlabel('Job Category')
ax.set_ylabel('Count')

# Rotate x-axis labels for better readability
plt.xticks(rotation=90)

# Show the chart
plt.show()


## Whats the salary distribution per job category?


df.createOrReplaceTempView("dfsql")
spark.sql("select distinct salaryFrequency from dfsql").show()

# Assuming 8 working hours per day and 250 working days per year
hours_per_day = 8
days_per_year = 250

df = df.withColumn(
    "annual_salary_from",
    when(col("salaryFrequency") == "hourly", col("salaryRangeFrom") * hours_per_day * days_per_year)
    .when(col("salaryFrequency") == "daily", col("salaryRangeFrom") * days_per_year)
    .otherwise(col("salaryRangeFrom")),
).withColumn(
    "annual_salary_to",
    when(col("salaryFrequency") == "hourly", col("salaryRangeTo") * hours_per_day * days_per_year)
    .when(col("salaryFrequency") == "daily", col("salaryRangeTo") * days_per_year)
    .otherwise(col("salaryRangeTo")),
)


df = df.withColumn("AverageSalary",((col("annual_salary_from")+col("annual_salary_to"))/2))

df.groupBy("jobCategory").agg(mean("AverageSalary").alias("AverageSalaryMean"),\
                              min("AverageSalary").alias("AverageSalaryMin"),\
                              max("AverageSalary").alias("AverageSalaryMax"),\
                             ).orderBy(col("AverageSalaryMean").desc()).limit(10).show()

## Is there any correlation between the higher degree and the salary?


df.select("Level").distinct().orderBy(col("Level").desc()).show()

from pyspark.sql.types import IntegerType

# assume 0 to 4 are static, 4A must be 5,4B must be 6, M1 is 7 and so on.. 
# allowing all values in level to be numerical and in order
def standardize_levels(level):
    
    level = level.strip()
    
    if level is None:
        return None
    
    elif level.startswith("M"):
        return int(level[1:]) + 6
    
    elif level == "4A":
        return 5
    elif level == "4B":
        return 6
    else:
        return int(level)
    
standardize_levels_udf = udf(standardize_levels, IntegerType())

df = df.withColumn("standardizedLevel",standardize_levels_udf(col("level")))

df.describe("standardizedLevel").show()

correlation = df.stat.corr("standardizedLevel", "AverageSalary")
print("Correlation between Level and Average Salary:", correlation)

## There is a positive correlation between the degree and the salary



## Whats the job posting having the highest salary per agency?


df.createOrReplaceTempView("dfsql")
spark.sql("select Agency,jobId,businessTitle,AverageSalary from (\
select Agency,jobId,AverageSalary,businessTitle,row_number() over(partition by Agency order by AverageSalary desc) rnum\
    from dfsql) q where rnum=1").show(truncate=False)

## Whats the job positings average salary per agency for the last 2 years?


df.createOrReplaceTempView("dfsql")
spark.sql("select Agency,avg(averageSalary) from dfsql \
where to_date(substr(postingDate,1,10),'yyyy-MM-dd') > date_sub(current_timestamp(),730)\
group by Agency").show(truncate=False)

## The query works if we increase the date sub days, but there is no recent data in the dataset

## What are the highest paid skills in the US market?


from pyspark.ml.feature import Tokenizer, StopWordsRemover

## one way is to check the highest paid skills from the qualifications required
## below i have mapped all skills with the respective salary
## then grouped by the skill, order by desc would give us the highest paid skills

def extract_skills(df, input_col, output_col):
    tokenizer = Tokenizer(inputCol=input_col, outputCol="tokens")
    df = tokenizer.transform(df)
    
    remover = StopWordsRemover(inputCol="tokens", outputCol=output_col)
    df = remover.transform(df)
    
    return df

df = extract_skills(df, "Minimum Qual Requirements", "min_qual_skills")

from pyspark.sql.functions import explode

df_skills = df.select("min_qual_skills", "AverageSalary").withColumn("skill", explode(col("min_qual_skills")))

highest_paid_skills = (
    df_skills.groupBy("skill")
    .agg(mean("AverageSalary").alias("average_salary"))
    .orderBy(col("average_salary").desc())
    .limit(10)
)

highest_paid_skills.show(truncate=False)

