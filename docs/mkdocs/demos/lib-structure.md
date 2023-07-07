# Library Structure

To understand more about *sparksnake* and all its advantages, this section will cover details about the library construction and the relationship between its modules and classes. At the end, you will find updates about usage demos recorded to provide a clear view of some of library features.

## A special way to enhance ETL jobs

At first, it is important to mention that the main point of interaction between users and *sparksnake* library takes place through the class `SparkETLManager` from `manager` module.

In other words, it means that any Spark application that wants to be built using *sparksnake* need to have the `SparkETLManager` class as a way to obtain all the library features, regardless on where the application will be deployed (locally or in AWS using Glue or EMR, for example).

???+ question "But how does this happen in practice?"
    Essentially, when users initialize an object of `SparkETLManager` class, they must pass an attributed called `mode`. Such attribute has the responsibility to "configure" the class according to the target service claimed to be used in the definition of their Spark application to be deployed.
    
    In other words, the *sparksnake* library will be able to provide special features for users even if they are building a Spark application to run on Glue, on EMR or even locally. Just set up a `mode` on `SparkETLManager` class and start building!

    In technical terms, it is through the "operation mode" passed by the user on `mode` attribute that the class can inherit functionality from other classes in the library. For example, if the user creates an object of the class `SparkETLManager` with `mode="glue"`, then all attributes and methods of `GlueJobManager` class on `glue` module will be inherited to provide a custom Spark usage experience within the specifics of the Glue service.

Just to summarize, consider the following diagram that shows how *sparksnake* can be used to improve users experience on developing Spark applications.

![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/imgs/sparksnake-draw.png)

And now, bringing some code snippets into the game, let's see some examples of starting up the `SparkETLManager` in different operation modes. The block code below shows one simple cases when users needs to use sparksnake's features in any environment (**default** operation mode) and another one where users needs to use the package's features to deploy Spark applications as Glue jobs in AWS (**glue** operation mode).

???+ example "Using SparkETLManager class as central point of Spark applications development"

    ```python
    # Importing libraries
    from sparksnake.manager import SparkETLManager

    # Initializing class to enable the development of Spark apps anywhere
    spark_manager = SparkETLManager(mode="default")

    # OR using Spark as Glue jobs in AWS
    spark_manager = SparkETLManager(mode="glue", **kwargs)
    ```

    Here, `**kwargs` argument represents addicional arguments that will be eventually needed according to each class service inherited.

If anything still seems complex up here, no worries! There will be a special section full of practical examples and video demonstrations to make everything as clear as possible!

## Library modules and classes

Now that you have a basic knowledge about the features of *sparksnake* library, the table below will help you even more to clarify all classes and modules available up to now.

| **Python Class Available** | **Module** | **Description** |
| :-- | :-- | :-- |
| `SparkETLManager` | `manager` | Central point of interaction with users. Inherits attributes and features from other classes according to the configured mode of operation |
| `GlueJobManager` | `glue` | 	Centralizes specific attributes and methods to be used within AWS Glue |

## Hands on demos

By the end, there is nothing more illustrativo that a hands on section with real examples and day to day scenarios. So, let's deep dive into those demos to know more about all the features that are available to users in order to take the Spark application development to another level!

**Feature demos**

- :material-alert-decagram:{ .mdx-pulse .warning } [Extracting date attributes using the `date_transform()`](features/date_transform.md)
- :material-alert-decagram:{ .mdx-pulse .warning } [Aggregating data using `agg_data()`](features/agg_data.md)
- :material-alert-decagram:{ .mdx-pulse .warning } [Repartitioning DataFrames to improve storage with `repartition_dataframe()`](features/repartition_dataframes.md)
- :material-alert-decagram:{ .mdx-pulse .warning } [Running SparkSQL queries in sequence with `run_spark_sql_pipeline()`](feature/run_spark_sql_pipeline.md)

**End to end examples**

- [Enhancing and optimizing the development of Glue jobs](end-to-end/demo-glue.md)
