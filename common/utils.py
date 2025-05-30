from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit

def create_hub(df, business_keys, hub_name):
    """Create a Hub with hash key, load_date, and record_source."""
    hub_df = df.select(*business_keys).dropDuplicates()
    hub_df = hub_df.withColumn(
        "hash_key", sha2(concat_ws("||", *[df[c] for c in business_keys]), 256)
    ).withColumn(
        "load_date", current_timestamp()
    ).withColumn(
        "record_source", lit(hub_name)
    )
    return hub_df

def create_satellite(df, parent_key, attributes, sat_name):
    """Create a Satellite with business attributes."""
    sat_df = df.select(parent_key, *attributes).dropDuplicates()
    sat_df = sat_df.withColumn("load_date", current_timestamp()) \
                   .withColumn("record_source", lit(sat_name))
    return sat_df
