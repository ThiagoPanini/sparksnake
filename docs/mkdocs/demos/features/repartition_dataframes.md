# Feature Demo: Repartition DataFrames

Welcome to this demo page where we will go through another *sparksnake* feature that enables users to repartition their Spark DataFrame objects in order to improve and optimize storage in distributed systems such as Amazon S3.

| | |
| :-- | :-- |
| 🚀 **Feature** | Repartitioning of Spark DataFrames objects in a fancy way |
| 💻 **Method** | [SparkETLManager.repartition_dataframe()](../../mkdocstrings/sparketlmanager.md#sparksnake.manager.SparkETLManager.repartition_dataframe) |
| ⚙️ **Operation Mode** | Available in all operation modes |

___

## SparkETLManager class setup

Before we move to the method demonstration, let's import and initialize the `SparkETLManager` class from `sparksnake.manager` module. As this feature is presented in the sparksnake's default mode (and so any operation mode can use it too), the class initialization is quite simple.

??? example "Importing and initializing the SparkETLManager class"
    
    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/date_transform_01.gif)

    🐍 **Code:**

    ```python
    # Importing libraries
    from sparksnake.manager import SparkETLManager

    # Starting the class
    spark_manager = SparkETLManager(
        mode="default"  # or any other operation mode
    )
    ```

So, assuming we already have a Spark DataFrame object `df_payemnts` with the attributes shown in the previous section, we can finally start the demo showing some powerful ways to enhance repartitioning proccesses in Spark DataFrames using the `repartition_dataframe()` method.

## The `repartition_dataframe()` method

From now on, we will deep dive into the possibilities delivered by the `repartition_dataframe()` method. For each new subsection of this page, a different application of the method will be shown with a hands on demo. The idea is to provide a clear view of all possibilities available in the method and to show everything that can be done with it.

If you haven't already taken a look at the [method's documentation](../../mkdocstrings/sparketlmanager.md#sparksnake.manager.SparkETLManager.repartition_dataframe), take your chance to understand how we will configure its parameters in order to achieve all of our goals. Just to summarize it, when calling the `repartition_dataframe()` method we have the following parameters to configure:

- `df` as a target DataFrame object to be repartitioned
- `num_partitions` to set a new partition required number

???+ tip "Repartitioning DataFrames using sparksnake's method"

    The `repartition_dataframe()` method is quite simple to understand, it works as follows:

    1. The current number of partitions in the target DataFrame is checked
    2. The desired number of partitions passed as a parameter is checked
    3. If the desired number is LESS than the current number, then the method `coalesce()` is executed
    4. If the desired number is GREATER than the current one, then the method `repartition()` is executed

So, the demos provided will only show different use cases for the method using a sample DataFrame already available in memory. We will primarly try to increase and decrease the number of partitions for the given DataFrame and see if the method works as expected.

Finally, the first thing we must do before is to check the current number of partitions of the sample DataFrame that will be used on demos. For that, you can copy and run the following command:

```python
# Getting the current number of partitions of a given DataFrame
current_partitions = df.rdd.getNumPartitions()
```

In this cases, we are using a DataFrame object that has **4 partitions**. Let's now use the `repartition_dataframe()` method to change this behavior according to our needs.

### Increasing the number of partitions in a DataFrame

The first try we will do is to increase the number of partitions of our sample DataFrame.

??? example "Increasing the partitions in a DataFrame"
    
    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/repartition_dataframe_01.gif)

    🐍 **Code:**

    ```python
    # Increasing partitions
    df_increase_partitions = spark_manager.repartition_dataframe(
        df=df,
        num_partitions=10
    )

    # Checking the result
    print(f"Partitions before the repartition: {df.rdd.getNumPartitions()}")
    print(f"Partitions after the repartition: {df_increase_partitions.rdd.getNumPartitions()}")
    ```

The great take away of the sparksnake's repartition method is that it handles which pyspark method best fits to do the task according to the current number of the target DataFrame and the desired number of partitions, as stated on the tip block above. If you don't have many information about partitions in a given DataFrame, the `repartition_method()` can be a good fit for you.

### Decreasing the number of partitions in a DataFrame

Well, let's now see an example of decreasing the number of partitions in a Spark DataFrame. In this case, let's try to use the sparksnake's repartition method do decrease the number of partitions from 4 to 2 in a given DataFrame.

??? example "Decreasing the partitions in a DataFrame"
    
    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/repartition_dataframe_02.gif)

    🐍 **Code:**

    ```python
    # Decreasing partitions
    df_decrease_partitions = spark_manager.repartition_dataframe(
        df=df,
        num_partitions=2
    )

    # Checking the result
    print(f"Partitions before the repartition: {df.rdd.getNumPartitions()}")
    print(f"Partitions after the repartition: {df_decrease_partitions.rdd.getNumPartitions()}")
    ```

### Trying to repartition a DataFrame with the same number of partitions

Now, we said that if you try to **increase** the number of partitions in a DataFrame, the `repartition_method()` will run the pyspark's `repartition()` method. By the other hand, if you try to **decrease** the number of partitions, the package method will run `coalesce()` to do the task.

But what if you try to change the number of partitions and pass a desired number that is roughly the **same** as the current number of partitions of a DataFrame?

In fact, the `repatition_method()` is prepared to check this condition and simply **pass** the task. In other words, the same DataFrame is returned by the users without doing any repartition task. It can save a little bit of time if you don't know the current number of partitions in your Spark DataFrame.

??? example "Trying to repartition by the current number of partitions"
    
    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/repartition_dataframe_03.gif)

    🐍 **Code:**

    ```python
    # Keeping the same number of partitions
    df_same_partitions = spark_manager.repartition_dataframe(
        df=df,
        num_partitions=4
    )

    # Checking the result
    print(f"Partitions before the repartition: {df.rdd.getNumPartitions()}")
    print(f"Partitions after the repartition: {df_same_partitions.rdd.getNumPartitions()}")
    ```

___

And that's it for the `repartition_dataframe()` method demo! I hope this one can be a good way to enrich your Spark applications that needs to optimize storage steps!