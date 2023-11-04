# ____________________________________________________________________
import subprocess
import sys
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# install('pymongo') #to install any python package
#---------------------------------------------------------------------


from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pymongo import MongoClient
import json

spark = SparkSession.builder.appName("Spa-Assignment").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for parsing the JSON data
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("coordinates", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("distanceFromMall", StringType(), True),
    StructField("clusterCategory", StringType(), True)
])

# Create a MongoDB client and connect to the MongoDB server
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["spa_assignment"]
collection = db["spa_assignment_collection"]


# Read data from Kafka into a DataFrame
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spa_assignment_topic") \
    .load()


def save_to_mongodb(batch_df: DataFrame, batch_id: int):
    # Use foreach to iterate over each row in the batch
    for row in batch_df.toLocalIterator():
        # Parse the JSON data from the 'value' field
        value_json = json.loads(row.value.decode("utf-8"))

        # Extract the 'customer_id' field from the parsed JSON
        customer_id = value_json.get("customer_id")

        # Check if 'customer_id' is None or empty, and skip processing if needed
        if customer_id is None or customer_id == "":
            continue

        # Define the filter condition to identify existing documents (using a unique identifier)
        filter_condition = {"customer_id": customer_id}

        # Define the update operation to update the existing document or insert a new one
        update_operation = {
            "$set": {
                "customer_id": customer_id,
                "coordinates": value_json.get("coordinates"),
                "timestamp": value_json.get("timestamp"),
                "distanceFromMall": value_json.get("distanceFromMall"),
                "clusterCategory": value_json.get("clusterCategory")
            }
        }

        # Use the upsert=True option to perform an upsert operation
        collection.update_one(filter_condition, update_operation, upsert=True)

# Usage for each Batch to save data with update
df.writeStream.foreachBatch(save_to_mongodb).outputMode("append").start()

    
# Parse the JSON data within the "value" column using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed_df = parsed_df.withColumn("distanceFromMall", col("distanceFromMall").cast(DoubleType()))
parsed_df = parsed_df.withColumn("clusterCategory", col("clusterCategory").cast(IntegerType()))

# Print the DataFrame to the console
console_query = parsed_df.writeStream.outputMode("append").format("console").start()

# Initialize state for maintaining the last observed distance
initial_state = {}
offer_timestamps = {}
# dict of category:offerType
category_offers = {
    0: "Offer A",
    1: "Offer A",
    2: "Offer B",
    3: "Offer C",
    4: "Offer C"
}

def update_state_and_check_eligibility(batch_df, batch_id):
    eligibility = "Not Eligible for Offer" # default.
    offer = ""
    print(f'============================================PROCESSING BATCH {batch_id} ==================================================')
    for row in batch_df.rdd.collect():

        customer_id = row.customer_id

        print(f'------------Processing customer {customer_id} from batch {batch_id}---------------')
        
        distance = row.distanceFromMall
        
        print(f'Current distance of the customer {customer_id} is {distance} ')
        last_distance = initial_state.get(customer_id, None)
        print(f'Last known distance of the customer {customer_id} is {last_distance} ')
        last_offer_timestamp = offer_timestamps.get(customer_id, None)
        print(f'Last generated offer for the customer {customer_id} was at {last_offer_timestamp} ')

        # Check if the distance is decreasing and decide eligibility
        if last_distance is not None and distance <= last_distance:
            # row_timestamp = datetime.strptime(row.timestamp, "%Y-%m-%d %H:%M:%S")
            row_timestamp = datetime.strptime(row.timestamp, "%Y-%m-%d %H:%M:%S.%f")
            if last_offer_timestamp is not None:
                last_offer_timestamp = datetime.strptime(last_offer_timestamp, "%Y-%m-%d %H:%M:%S.%f")
                time_difference = (row_timestamp - last_offer_timestamp).total_seconds()
                print(f'time_difference since last offer is {time_difference}')
                # Check if an offer was generated in the last 5 minutes (window and filter it to avoid spamming)

                if time_difference <= 10:  # 5 minutes in seconds
                    eligibility = "Not Eligible for Offer (Offer generated in the last 5 minutes)"
                else:
                    customer_category = int(row.clusterCategory)
                    print(f'CUSTOMER {customer_id} ELIGIBLE FOR OFFER {category_offers[customer_category]}')
                    offer_timestamps[customer_id] = row.timestamp
                    eligibility = f'ELIGIBLE FOR OFFER {category_offers[customer_category]}'
            else:
                customer_category = int(row.clusterCategory)
                print(f'CUSTOMER {customer_id} ELIGIBLE FOR OFFER {category_offers[customer_category]}')
                offer_timestamps[customer_id] = row.timestamp
                eligibility = f'ELIGIBLE FOR OFFER {category_offers[customer_category]}'

        # Update the state with the new distance
        initial_state[customer_id] = distance

        # Print or further process the eligibility result
        print(f"Customer ID: {customer_id}, Distance: {distance}, Eligibility: {eligibility}")

# Apply stateful transformation and check eligibility using foreachBatch
mapped_with_state = parsed_df.select("customer_id", "distanceFromMall", "timestamp", "clusterCategory").writeStream.foreachBatch(update_state_and_check_eligibility).outputMode("append").start()

# Start the streaming query
mapped_with_state.awaitTermination()
