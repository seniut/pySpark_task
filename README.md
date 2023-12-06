In this exercise, you will be asked to develop a data engineering pipeline that works on entertainment industry data. Data is provided in data.zip file. The pipeline would be responsible for basic data preparation and building a few simple features on top of it.

As most of our data pipelines run on AWS Glue we would like you to prepare a solution for this task using Docker prepared by AWS.
Please note that AWS Glue provides Spark Environment with additional libraries which you are NOT required to use to provide solution for this task.
Here is an AWS documentation reference: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image
We recommend using image dedicated to AWS Glue version 4.0 with PySpark 3.3.0, but solutions with AWS Glue version 2.0 and PySpark 2.4.3 will be accepted as well.

Please use it to prepare your local development environment.
In the solution you are required to provide either a bash script which will run your solution using provided docker or a file describing steps to run it.


# Data.zip

## Content
This is a dataset with history of what people have watched.
The data contains 2 top level files:
* entities.csv which contains movie / tv-show information, id and additional metadata.
* people.csv which contains all people who uploaded watch history with their demographics information
And full history of all watched shows for each person in transactions folder.
Each file has data about one person - their watch history.


## Files
entities.csv:
* id: int unique identifier of movie / tv-show
* title: str: title of movie / tv-show
* genre: str: can be more than one at once, separated by |

people.csv:
* id: int unique identifier of person
* age: int
* gender: male | female

transactions:
Each file is a history of all watched entities by a person.
* person_id : id, matches with person.csv
* entity_id : id, matches with entities.csv
* date : when entity was watched by a person
* transaction_id : unique id of uploaded history (UUID)
Note: Every person is allowed to upload full history data only one time.

Tasks:
1. Denormalize data and store it in parquet format. Apply best storage practices. Assume that this code will run on larger scale later. This task should be done as a script. After denormalization the genre should have array type, and Age be in two formats: int and age-groups (one of: 18-29, 30-49, 50-69, 70+). Ensure all columns are in correct type.
2. This data assumes that there were fraudulent history uploads. Try to find fraudulent uploads and remove them from the dataset.
3. Answer following questions:
* what are top 10 most commonly watched entities
* what are top 5 most commonly watched genres
* what is most watched drama by females in age 18-29

It will be a plus if you use AWS Glue library (included in the image)

Focus on what's the most important. Show us where you are best.


# Development
To be honest, I didn't have enough time to make an adequate decision. I spent a lot of time setting up the local environment to work through pyCharm with AWS Glue Job (I usually make it directly in Glue Studio). 
Also, I don't have own AWS account to work with S3 (as I know Glue Jobs cannot work with local files).
After that I tried just use pySpark locally and I had smale time to make something. 
So, there is dirty and untested solution (this is more like a logical approach):
https://github.com/seniut/pySpark_task