import great_expectations as gx
from pyspark.sql import SparkSession
from great_expectations.checkpoint import (SlackNotificationAction, UpdateDataDocsAction,)
import os

from pyspark.sql.functions import col

src_data = "test_data/movies.csv"

########################################################
# 1. Use Spark to load movies.csv into a DataFrame (DF)#
########################################################
spark = SparkSession.builder \
    .appName("Integrate Spark Job with GX") \
    .getOrCreate()

print("SparkSession Created.")

spark_df = spark.read.csv(src_data,header=True).withColumn("age", col("age").cast("int"))

print("\nSpark DataFrame loaded from CSV:")

# Show csv file after loaded into DF
spark_df.show()
spark_df.printSchema()

########################################################
# 2. Use the Above Spark DF as the Data Source for GX  #
########################################################

# Define where the project created and save the configure (yaml, json)
root_dir="validate_results"

# Retrieve your Data Context
context = gx.get_context(mode="file", project_root_dir=root_dir)

# Define the Data Source name
data_source_name = "messi655_data_source"

# Add the Data Source to the Data Context
data_source = context.data_sources.add_spark(name=data_source_name)

# Define the Data Asset name
data_asset_name = "messi655_data_asset"

# Add a Data Asset to the Data Source
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# Define the Batch Definition name
batch_definition_name = "messi655_batch_definition"

# Add a Batch Definition to the Data Asset
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)

#####################################################################
# 3. Define the Expectations that will be used to validate the data #
#####################################################################
# Create an Expectation Suite
suite_name = "messi655_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)

# Add the Expectation Suite to the Data Context
suite = context.suites.add(suite)

batch_parameters = {"dataframe": spark_df}

# Check Column Not Null
suite.add_expectation(
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="movieId")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="title")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="genres")
)

# Check Row Count
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToEqual(value=15)
)

suite.add_expectation(
    gx.expectations.ExpectColumnMaxToBeBetween(column="age",
                    min_value=18,
                    max_value=21)
)

########### Defination Validation

# Create a Validation Definition
definition_name = "messi655_validation_definition"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name=definition_name
)

# Add the Validation Definition to the Data Context
context.validation_definitions.add(validation_definition)

base_directory = "./uncommitted/data_docs/messi655_local_site/"
site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": base_directory,
    },
}

site_name = "messi655_data_docs_site"
context.add_data_docs_site(site_name=site_name, site_config=site_config)

# Create a list of Actions for the Checkpoint to perform
action_list = [
    gx.checkpoint.actions.UpdateDataDocsAction(
        name="update_messi655_site", site_names=[site_name]
    )
]

#####################
# 3. Run validation #
#####################
# Create the Checkpoint
checkpoint_name = "messi655_checkpoint"
checkpoint = gx.Checkpoint(
    name=checkpoint_name,
    validation_definitions=[validation_definition],
    actions=action_list,
    result_format={"result_format": "COMPLETE"},
)

# Save the Checkpoint to the Data Context
context.checkpoints.add(checkpoint)

validation_results = checkpoint.run(
    batch_parameters=batch_parameters, expectation_parameters=suite
)

# View the Data Docs
context.open_data_docs()

print(validation_results)