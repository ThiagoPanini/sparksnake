# Library Structure

To understand a little bit more about *sparksnake* under the hood and all its advantages, this section will cover details about the library construction construction and the relationship between its modules and classes.

## A Special Way to Enhance ETL Jobs

At first, it is important to mention that the main point of interaction between users and the *sparksnake* library takes place through the class `SparkETLManager` set in the module `manager`.

It means that it's through this class that users will be able to acquire a series of already coded Spark features and specific methods that are intended to help to reduce the "hard part" of using AWS services like Glue and EMR.

???+ question "But how does this happen in practice?"
    Essentially, when users initialize an object of class `SparkETLManager`, they must pass an attributed called `mode`. Such attribute has the responsibility to "configure" the class according to the target service claimed to be used in the definition of their Spark application to be deployed.
    
    In other words, the *sparksnake* library will be able to provide special features for users even if they are building a Spark application to run on Glue, on EMR or even locally. Just set up a `mode` on `SparkETLManager` class and start building!

    In technical terms, it is through the "operation mode" passed by the user on `mode` attribute that the class can inherit functionality from other classes in the library. For example, if the user instancies an object of the class `SparkETLManager` with `mode="glue"`, then all attributes and methods of the class `GlueJobManager` on module `glue` will be inherited to provide a custom Spark usage experience within the specifics of the Glue service.

In visual terms, the library usage journey can be simplified in the three steps below:

![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/imgs/sparksnake-draw.png)

To illustrate all this with practical coding (I know you like it), the snippet below shows how the `SparkETLManager` class can be used to centralize the use of Spark across different AWS services:

???+ example "Using SparkETLManager class as central point of Spark application development"

    ```python
    # Importing libraries
    from sparksnake.manager import SparkETLManager

    # Initializing class for building Spark applications on Glue
    spark_manager = SparkETLManager(mode="glue", **kwargs)

    # OR on EMR
    spark_manager = SparkETLManager(mode="emr", **kwargs)

    # OR even locally
    spark_manager = SparkETLManager(mode="local")
    ```

    Here, `**kwargs` argument represents addicional arguments that will be eventually needed according to each class service inherited.

If anything still seems complex up here, no worries! There will be a special section full of practical examples and video demonstrations for making everything as clear as possible!

## Library Modules and Classes

Now that you have a basic knowledge about the features of *sparksnake* library, the table below will help you even more to clarify all classes and modules available up to now.

| **Python Class Available** | **Module** | **Description** |
| :-- | :-- | :-- |
| `SparkETLManager` | `manager` | Central point of interaction with users. Inherits attributes and features from other classes according to the configured mode of operation |
| `GlueJobManager` | `glue` | 	Centralizes specific attributes and methods to be used within AWS Glue |
| `EMRJobManager` | `emr` | :hammer_and_wrench: *Work in progress* |

## Usage Demos

Examples and demos will be divided in different sections according to target AWS service chosen for *sparksnake* usage. Up to now, the following scenarios are available:

- :material-alert-decagram:{ .mdx-pulse .warning } [Enhancing and optimizing the development of Glue jobs](exemplos-glue.md)
