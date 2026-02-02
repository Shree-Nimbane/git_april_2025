from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, struct


def test_count():


    spark = SparkSession.builder.getOrCreate()

    data = [
        (1, ("john", ("mumbai", 400001))),
        (2, ("ravi", ("pune", 411001)))
    ]
    schema = "id INT, person STRUCT<name:STRING, address:STRUCT<city:STRING, pincode:INT>>"

    df = spark.createDataFrame(data, schema)
    df.show(truncate=False)

    df2 = df \
        .withColumn(
        "persona", struct(upper(col("person.name")).alias("name"), struct(col("person.address.pincode").alias("pincode")
                                                                          ).alias("address")
                          )
    )



def test_count_2():
    assert 1 == 1
