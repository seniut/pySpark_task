# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf, col
from pyspark.sql.types import StringType, IntegerType, ArrayType


# UDF:
def age_to_group(age):
    if age < 30:
        return "18-29"
    elif age < 50:
        return "30-49"
    elif age < 70:
        return "50-69"
    else:
        return "70+"

# # Initialize Spark and Glue contexts
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
#
# # Begin Glue job
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)


# Initialize Spark session
spark = SparkSession.builder.appName("DataIntegration").master("local[*]").getOrCreate()

# Define the path to the directory containing the CSV files
trans_csv_files = "./data/transactions/*"
entities_csv_path = "./data/entities.csv/"
people_csv_path = "./data/people.csv"

transactions_df = spark.read.csv(trans_csv_files, header=True, inferSchema=True)
entities_df = spark.read.csv(entities_csv_path, header=True, inferSchema=True)
people_df = spark.read.csv(people_csv_path, header=True, inferSchema=True)


## For reading data by Glue from S3:
# # Define the path to the CSV files
# csv_file_path = "s3://your-bucket/path/to/csv/files/"
#
# # Read all CSV files from the specified path
# dynamic_frame = glueContext.create_dynamic_frame.from_options(
#     format_options={"withHeader": True},
#     connection_type="s3",
#     format="csv",
#     connection_options={"paths": [csv_file_path]}
# )


######## fraudulent data ################
## same_age and gender_diff_persons ##
# Define a window partitioned by age and gender
windowSpec = Window.partitionBy("age", "gender").orderBy("id")
# Add a count of person_ids per age and gender group
people_df = people_df.withColumn("row_number_person_ids", f.row_number().over(windowSpec))
# people_df.show()
# Filter out rows where row number is greater than 1 (i.e., keep only the first occurrence)
same_age_gender_diff_persons_df = people_df.filter(people_df["row_number_person_ids"] > 1)
# same_age_gender_diff_persons_df.show()

# # Cast 'genre' from ArrayType to StringType by joining the array elements with a separator
# same_age_gender_diff_persons_df = same_age_gender_diff_persons_df.withColumn("genre", f.concat_ws("|", "genre"))

# Store these rows in a separate file
same_age_gender_diff_persons_df.write.csv("./fraudulent/same_age_gender_diff_persons", header=True, mode='overwrite')

# # Drop the extra column
# people_df = people_df.drop("row_number_person_ids")


### inadequate age values ###
# Filter rows with inadequate age values
inadequate_age_df = people_df.filter((people_df["age"] < 0) | (people_df["age"] > 100))
# inadequate_age_df.show()

# Store these rows in a separate file
inadequate_age_df.write.csv("./fraudulent/inadequate_age", header=True, mode='overwrite')


# Deleting rows with the same age and gender but different person_ids
people_df = people_df.join(same_age_gender_diff_persons_df, ["id", "age", "gender"], "left_anti")

# Deleting rows with inadequate age values
people_df = people_df.join(inadequate_age_df, ["id", "age", "gender"], "left_anti")


# Data processing

# Join transactions_df with people_df on person_id and id
denormalized_df = transactions_df.join(people_df, transactions_df.person_id == people_df.id)

# Join the result with entities_df on entity_id and id
denormalized_df = denormalized_df.join(entities_df, denormalized_df.entity_id == entities_df.id)

# we can drop the duplicate id columns after the join if needed
denormalized_df = denormalized_df.drop(people_df.id).drop(entities_df.id)

age_group_udf = udf(age_to_group, StringType())
denormalized_df = denormalized_df.withColumn("age_group", age_group_udf(col("age")))

# Ensure all columns are in correct type
denormalized_df = denormalized_df.withColumn("person_id", col("person_id").cast(IntegerType()))
denormalized_df = denormalized_df.withColumn("entity_id", col("entity_id").cast(IntegerType()))

# Convert 'genre' to array type
denormalized_df = denormalized_df.withColumn("genre", split("genre", "\|").cast(ArrayType(StringType())))

### Top 10 Most Commonly Watched Entities ###
# Group by entity_id and count the occurrences
top_entities = denormalized_df.groupBy("title").count()
# Sort in descending order and take the top 10
top_10_entities = top_entities.orderBy(f.desc("count")).limit(10)
# Store the results in a separate file
# top_10_entities.show()
top_10_entities.write.csv("./result/top_10_entities", header=True, mode='overwrite')

### Top 5 Most Commonly Watched Genres ###
# Explode the genre array into individual genre rows
exploded_genres = denormalized_df.withColumn("genre", f.explode("genre"))
# Group by genre and count occurrences
top_genres = exploded_genres.groupBy("genre").count()
# Sort in descending order and take the top 5
top_5_genres = top_genres.orderBy(f.desc("count")).limit(5)
# Store the results in a separate file
# top_5_genres.show()
top_5_genres.write.csv("./result/top_5_genres", header=True, mode='overwrite')

### Most Watched Drama by Females Aged 18-29 ###
# Filter for females aged 18-29 and where genre is 'drama'
# Filter for females aged 18-29 and where genre array contains 'drama'
drama_female_18_29 = denormalized_df.filter(
    (denormalized_df["gender"] == "female") &
    (denormalized_df["age"] >= 18) &
    (denormalized_df["age"] <= 29) &
    f.array_contains(denormalized_df["genre"], "drama")
)
# Group by entity_id and count occurrences
most_watched_drama = drama_female_18_29.groupBy("title").count()
# Sort in descending order and take the top 1
top_drama_female_18_29 = most_watched_drama.orderBy(f.desc("count")).limit(1)
# top_drama_female_18_29.show()
# Store the results in a separate file
top_drama_female_18_29.write.csv("./result/top_drama_female", header=True, mode='overwrite')


# Write the result to Parquet
output_path = "./result/result_parquet"
denormalized_df.write.mode("overwrite").partitionBy("age_group").parquet(output_path)

# # Write out the results to Parquet into S3
# glueContext.write_dynamic_frame.from_options(frame = entities_dyf, connection_type = "s3", connection_options = {"path": "s3://path/to/output/entities"}, format = "parquet")
# glueContext.write_dynamic_frame.from_options(frame = people_dyf, connection_type = "s3", connection_options = {"path": "s3://path/to/output/people"}, format = "parquet")

# Complete the job
# job.commit()

spark.stop()
