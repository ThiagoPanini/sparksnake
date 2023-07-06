# Feature Demo: Extracting Date Attributes

Welcome to this demo where we will go through a special *sparksnake* feature: the possibility to extract multiple date attributes from a Spark DataFrame date column.

In order to provide an overview for what we will show in this demo, let's consider the table below:

| | |
| :-- | :-- |
| 🚀 **Feature** | Extraction of multiple date attributes from a date column |
| 💻 **Method** | [SparkETLManager.date_transform()](../../mkdocstrings/sparketlmanager.md#sparksnake.manager.SparkETLManager.date_transform) |
| ⚙️ **Operation Mode** | Available in all operation modes |

___

## Data and Context

First of all, let's introduce the data used on this demo as part of the feature application. For this, we are talking about a simplified version of the [orders](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_orders_dataset.csv) dataset with the following schema:

```
root
 |-- order_id: string (nullable = true)
 |-- order_purchase_ts: string (nullable = true)
```

So, the DataFrame consists of only two columns, one of them showing an unique order id and the other one with the exact timestamp of an order purchase. This timestamp information will be our main target in this demo as we have the opportunity to extract some date attributes from it. As a last step in this introduction section, let's see some rows of this DataFrame.

```
+--------------------------------+-----------------+
|order_id                        |order_purchase_ts|
+--------------------------------+-----------------+
|e481f51cbdc54678b7cc49136f2d6af7|02/10/2017 10:56 |
|53cdb2fc8bc7dce0b6741e2150273451|24/07/2018 20:41 |
|47770eb9100c2d0c44946d9cf07ec65d|08/08/2018 08:38 |
|949d5b44dbf5de918fe9c16f97b45f8a|18/11/2017 19:28 |
|ad21c59c0840e6cb83a9ceb5573f8159|13/02/2018 21:18 |
+--------------------------------+-----------------+
only showing top 5 rows
```

Well, it seems that we have the following scenario:

- An attribute with timestamp information
- This attribute are originally in a string date type format
- And finally, this attribute has a date format presented as "dd/MM/yyyy HH:mm"

???+ question "So, what if we want to answear the following questions:"
    1. What if we need to extract the order purcharse year in a different DataFrame column?
    2. What if we also need to extract the order purchase month in a different column?
    3. And what if we need to extract some other date attribute such as day, week of day, quarter and any other possible date attributes available in pyspark?
    4. Can we just do that with a single method execution or do we need to create a complex `selectExpr` statement with casting functions?

Let's see the `date_transform()` method in action!

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
        mode="default"
    )
    ```

So, assuming we already have a Spark DataFrame object `df_orders` with the attributes shown in the previous section, we can finally start the demo showing some powerful ways to enhance the extraction of date attributes from date columns using the `date_transform()` method.

## The `date_transform()` method

From now on, we will deep dive into the possibilities delivered by the `date_transform()` method. For each new subsection, a differente application of the method will be shown so users can take a look at everything that can be done with it.

If you haven't already taken a look at the [method's documentation](../../mkdocstrings/sparketlmanager.md#sparksnake.manager.SparkETLManager.date_transform), take your chance to understand how we will configure its parameters in order to achieve our goal. Speaking of them, in this application example, we will use the following method parameters:

- `df` to pass a target Spark DataFrame to transform
- `date_col` to point the name of our date column (even if it's yet a string)
- `date_col_type` to say if the "date column" will be casted as a date or as a timestamp type
- `date_format` to configure the date or timestamp format for the result attribute
- `cast_string_to_date` to explicit say that we want to cast the string column into a date or timestamp column

### Casting a string column as date or timestamp

The first application example of the `date_transform()` method takes place on casting a string attribute that has date or timestamp information into date or timestamp types.

??? example "Casting a string column into a date or timestamp column"

    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/date_transform_02.gif)

    🐍 **Code:**

    ```python
    # Casting a date column in a source DataFrame
    df_orders_ts_cast = spark_manager.date_transform(
        df=df_orders,
        date_col="order_purchase_ts",
        date_col_type="timestamp",
        date_format="dd/MM/yyyy HH:mm",
        cast_string_to_date=True
    )

    # Showing the schema
    print("New DataFrame schema")
    df_orders_ts_cast.printSchema()

    # Showing some rows
    print("Sample of the new DataFrame")
    df_orders_ts_cast.show(5)
    ```

After applying the method on a source DataFrame with the parameters shown above, the returned DataFrame consist by essentialy the same attributes, but now with the `order_purchase_ts` casted as a timestamp.

```
New DataFrame schema
root
 |-- order_id: string (nullable = true)
 |-- order_purchase_ts: timestamp (nullable = true)

Sample of the new DataFrame
+--------------------+-------------------+
|            order_id|  order_purchase_ts|
+--------------------+-------------------+
|e481f51cbdc54678b...|2017-10-02 10:56:00|
|53cdb2fc8bc7dce0b...|2018-07-24 20:41:00|
|47770eb9100c2d0c4...|2018-08-08 08:38:00|
|949d5b44dbf5de918...|2017-11-18 19:28:00|
|ad21c59c0840e6cb8...|2018-02-13 21:18:00|
+--------------------+-------------------+
only showing top 5 rows
```


### Extracting year, month and all date attributes

So, the second and the last example of a `date_transform()` method feature is a huge one. Its behavior allows users to pass a date attribute from a Spark DataFrame and extract multiple date epochs in a single method call. The method's `**kwargs` have a great job in this feature as its can be used to configure which date epochs will be extracted from the date column.

Let's see, for instance, an usage example that shows the creation of a new column in the target DataFrame that has the order purchase year.

??? example "Adding a new column in the target DataFrame with the order purchase year"

    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/date_transform_03.gif)

    🐍 **Code:**

    ```python
    # Extracting the year from order purchase timestamp
    df_orders_purchase_year = spark_manager.date_transform(
        df=df_orders,
        date_col="order_purchase_ts",
        date_col_type="timestamp",
        date_format="dd/MM/yyyy HH:mm",
        cast_string_to_date=True,
        year=True
    )

    # Showing the schema
    print("New DataFrame schema")
    df_orders_purchase_year.printSchema()

    # Showing some rows
    print("Sample of the new DataFrame")
    df_orders_purchase_year.show(5)
    ```

The new DataFrame has the following schema and rows:

    ```
    New DataFrame schema
    root
    |-- order_id: string (nullable = true)
    |-- order_purchase_ts: timestamp (nullable = true)
    |-- year_order_purchase_ts: integer (nullable = true)

    Sample of the new DataFrame
    +--------------------+-------------------+----------------------+
    |            order_id|  order_purchase_ts|year_order_purchase_ts|
    +--------------------+-------------------+----------------------+
    |e481f51cbdc54678b...|2017-10-02 10:56:00|                  2017|
    |53cdb2fc8bc7dce0b...|2018-07-24 20:41:00|                  2018|
    |47770eb9100c2d0c4...|2018-08-08 08:38:00|                  2018|
    |949d5b44dbf5de918...|2017-11-18 19:28:00|                  2017|
    |ad21c59c0840e6cb8...|2018-02-13 21:18:00|                  2018|
    +--------------------+-------------------+----------------------+
    only showing top 5 rows
    ```

Well, let's now suppose we have the need to extract not only the year of a order purchase timestamp, but also the quarter, month and the day of the month. We can say that those are attributes that will guide stakeholders to make business decisions for some specific reason related to how ecommerce sales are going.

??? example "Extracting multiple date epochs to enrich a DataFrame"

    🎬 **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/date_transform_04.gif)

    🐍 **Code:**

    ```python
    # Extracting more date epochs from order purchase timestamp
    df_orders_purchase_date_epochs = spark_manager.date_transform(
        df=df_orders,
        date_col="order_purchase_ts",
        date_col_type="timestamp",
        date_format="dd/MM/yyyy HH:mm",
        cast_string_to_date=True,
        year=True,
        quarter=True,
        month=True,
        dayofmonth=True
    )

    # Showing the schema
    print("New DataFrame schema")
    df_orders_purchase_date_epochs.printSchema()

    # Showing some rows
    print("Sample of the new DataFrame")
    df_orders_purchase_date_epochs.show(5)
    ```

By adding new keyword arguments on the method call, the returned DataFrame has now some new date attributes to enrich our analysis

```bash
New DataFrame schema
root
 |-- order_id: string (nullable = true)
 |-- order_purchase_ts: timestamp (nullable = true)
 |-- year_order_purchase_ts: integer (nullable = true)
 |-- quarter_order_purchase_ts: integer (nullable = true)
 |-- month_order_purchase_ts: integer (nullable = true)
 |-- dayofmonth_order_purchase_ts: integer (nullable = true)

Sample of the new DataFrame
+--------------------+-------------------+----------------------+-------------------------+-----------------------+----------------------------+
|            order_id|  order_purchase_ts|year_order_purchase_ts|quarter_order_purchase_ts|month_order_purchase_ts|dayofmonth_order_purchase_ts|
+--------------------+-------------------+----------------------+-------------------------+-----------------------+----------------------------+
|e481f51cbdc54678b...|2017-10-02 10:56:00|                  2017|                        4|                     10|                           2|
|53cdb2fc8bc7dce0b...|2018-07-24 20:41:00|                  2018|                        3|                      7|                          24|
|47770eb9100c2d0c4...|2018-08-08 08:38:00|                  2018|                        3|                      8|                           8|
|949d5b44dbf5de918...|2017-11-18 19:28:00|                  2017|                        4|                     11|                          18|
|ad21c59c0840e6cb8...|2018-02-13 21:18:00|                  2018|                        1|                      2|                          13|
+--------------------+-------------------+----------------------+-------------------------+-----------------------+----------------------------+
only showing top 5 rows
```

___

And that's it for the `date_transform()` method demo! I hope this one can be a good way to enrich your Spark applications that uses DataFrames with date information!