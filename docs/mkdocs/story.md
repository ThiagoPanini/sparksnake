# Project Story

I always like to tell stories about open source solutions that I've been creating over time. With *sparksnake* that couldn't be any different.

🪄 So, this is the story about how the *sparksnake* project turned into reality for making everyone's life on developing Spark application way more easier.

## Learning Challenges

First of all, we are all living on this Big Data era for a long time. For years we are hearing about the challenges on data processing and about how big companies are trying to solve those problems using managed services, frameworks, programming languages and any other capable tool to handle the vast amount of data available nowadays.

For developers, the main take away is to understand that there will always be a series of tools to be learned in this Big Data era and this will probably not change for a long time. So, we just need to adapt ourselves and face this situation as best as possible.

![Juggling on a Simpson's episode](https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExMDZkZmJjM2EzOGMzMzVkNWVmMWI3ZjcxMjQ2NTljNzZlOTRjYzkxZCZjdD1n/3o6MbbUcnt8V46x2so/giphy.gif)

???+ note "But this isn't about the controversial life of a Data/Analytics Engineer"
    In fact, this is about optimizing the learning challenges we all have when facing new tools. This is a story on how real problems were solved using a couple of this "Big Data era" tools.


## Apache Spark, Cloud Computing and More Challenges

Well, all the introduction provided in the previous section could be summed up in one sentence: learning new things can be very painful sometimes. And now the real story starts by telling you about an analytics journey on using [Apache Spark](https://spark.apache.org/) to build ETL jobs locally or in AWS services like Glue and EMR.

???+ tip "Frameworks and analytics services in cloud providers"
    Today, the state-of-the-art on developing resilient and scalable applications revolves around cloud providers that are able to delivery a large range of services for almost every need. In the analytics industry, AWS services such as [AWS Glue](https://aws.amazon.com/glue/) and [Amazon EMR](https://aws.amazon.com/emr/) are some of the most used cloud services for developing applications that handle all the Big Data challenges in the modern world. Both of them use Apache Spark as the engine for processing data and this is the starting point around the motivation behind the creation of the *sparksnake* library.

I must say that learning Apache Spark was a rewarding task. It was really nice to see things happening when creating and running Spark applications. By the other hand, integrating Spark in AWS services like Glue needed more effort. For example, there were some particularities and specific points residing on Glue that must be considered for using the service. After all, not everyone who learn Spark know terms like `GlueContext`, `DynamicFrame` or `Job` inside Glue.

:material-alert-decagram:{ .mdx-pulse .warning } The main message is that no matter how big are the facilities delivered by these and other analytical frameworks and services, there will always be a learning curve that requires its users to understand some important technical details before diving into it and extracting all the potential delivered by such tools.

???+ quote "So the question was:"
    *"How to deliver an easy and low-complexity solution that can be used to develop Spark applications locally or inside AWS services allowing users to focus much more on applying their data transformation rules and much less on the complex service setup?"*

✨ The *sparksnake* library was about to born!

## A Mindset Change on Spark Application Development

Given the above scenario, let's use a real example using some real problems. Let's imagine we are trying to learn Glue and we have a first look on the AWS Management Console. We create a job with a boilerplate and this is what we see on the screen:

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
```

??? question "I think I knew Spark before, but what's all this?"
    The first thing to note is the `awsglue` library making presence with different modules, classes and functions. It's inevitable to make questions such as:

    - What's the function `getResolvedOptions`?
    - Why do I need a `GlueContext` and a `Job` element?
    - It seems like there are many lines of code to start something that could be started in a single line of code

It's not a problem to say that this Glue job setup may scary users a little bit. It scared me at the beginning, after all.

Even experienced users might be questioning something like: *"Every time I start a Glue job on AWS I must define this initial block of code. Would it be possible to simplify it by calling a single function?"*

:star:{ .heart } And here you can see some of the reasons *sparksnake* was born. There must be such a way to make everything easier for Spark users using the framework locally or in services like AWS Glue. For instance, one of its features is precisely to provide a way to initialize Glue jobs and get all context and session elements with a single line of code. All the boilerplate code above would be rewriten using *sparksnake* in the following way:

```python
# Importing the main class
from sparksnake.manager import SparkETLManager

# Initializing a class object and a Glue job
spark_manager = SparkETLManager(mode="glue", argv_list=["JOB_NAME"], data_dict={})
spark_manager.init_job()
```

There is no such `awsglue` libraries to handle and no complex elements or functions to configure. Everything comes down to creating a class object and calling methods.

Easy, isn't it? This is just one of a number of features specially created to facilitate the development journey of Spark applications integrated (or not) into Glue. Be sure to browse the documentation to absorb all this sea of possibilities and improve, once and for all, the process of creating jobs!
