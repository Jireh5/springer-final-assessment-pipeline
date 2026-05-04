from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Springer_Referral_Pipeline").getOrCreate()

DATA_DIR = "/app/data"

# BULLETPROOF SETTINGS: Ignores hidden spaces and catches all "Null" variations
read_opts = {
    "header": True, 
    "inferSchema": True, 
    "ignoreLeadingWhiteSpace": True, 
    "ignoreTrailingWhiteSpace": True, 
    "nullValue": "Null"
}

# Load data into DataFrames AND force all IDs to strings so they never crash during the joins
df_paid_transactions = spark.read.csv(f"{DATA_DIR}/paid_transactions.csv", **read_opts).withColumn("transaction_id", F.col("transaction_id").cast("string"))
df_referral_rewards = spark.read.csv(f"{DATA_DIR}/referral_rewards.csv", **read_opts).withColumn("id", F.col("id").cast("string"))
df_user_logs = spark.read.csv(f"{DATA_DIR}/user_logs.csv", **read_opts).withColumn("user_id", F.col("user_id").cast("string"))
df_user_referral_logs = spark.read.csv(f"{DATA_DIR}/user_referral_logs.csv", **read_opts).withColumn("user_referral_id", F.col("user_referral_id").cast("string"))
df_user_referral_statuses = spark.read.csv(f"{DATA_DIR}/user_referral_statuses.csv", **read_opts).withColumn("id", F.col("id").cast("string"))
df_user_referrals = spark.read.csv(f"{DATA_DIR}/user_referrals.csv", **read_opts).withColumn("referral_id", F.col("referral_id").cast("string")).withColumn("referrer_id", F.col("referrer_id").cast("string")).withColumn("referee_id", F.col("referee_id").cast("string"))
df_lead_log = spark.read.csv(f"{DATA_DIR}/lead_log.csv", **read_opts).withColumn("lead_id", F.col("lead_id").cast("string"))

# Data cleaning & Formatting
def apply_initcap(df, exclude_cols=[]):
    for col_name, dtype in df.dtypes:
        if dtype == 'string' and 'club' not in col_name.lower() and col_name not in exclude_cols:
            df = df.withColumn(col_name, F.initcap(F.col(col_name)))
    return df

df_user_logs = apply_initcap(df_user_logs)
df_user_referrals = apply_initcap(df_user_referrals, exclude_cols=['referral_id', 'referrer_id', 'referee_id'])

# Data Processing
# Join Tables
df_base = df_user_referrals.alias("ur") \
    .join(df_user_referral_logs.alias("url"), F.col("ur.referral_id") == F.col("url.user_referral_id"), "left") \
    .join(df_user_referral_statuses.alias("urs"), F.col("ur.user_referral_status_id") == F.col("urs.id"), "left") \
    .join(df_referral_rewards.alias("rr"), F.col("ur.referral_reward_id") == F.col("rr.id"), "left") \
    .join(df_paid_transactions.alias("pt"), F.col("url.source_transaction_id") == F.col("pt.transaction_id"), "left") \
    .join(df_user_logs.alias("referrer"), F.col("ur.referrer_id") == F.col("referrer.user_id"), "left") \
    .join(df_lead_log.alias("lead"), (F.col("ur.referral_source") == 'Lead') & (F.col("ur.referee_id") == F.col("lead.lead_id")), "left")

# Source Category Logic
df_base = df_base.withColumn("referral_source_category", 
    F.when(F.col("ur.referral_source") == 'User Sign Up', 'Online')
     .when(F.col("ur.referral_source") == 'Draft Transaction', 'Offline')
     .when(F.col("ur.referral_source") == 'Lead', F.col("lead.source_category"))
     .otherwise(None)
)

# Time Adjustment
df_base = df_base.withColumn(
    "transaction_at", 
    F.from_utc_timestamp(F.col("pt.transaction_at"), F.col("pt.timezone_transaction"))
).withColumn(
    "referral_at", 
    F.from_utc_timestamp(F.col("ur.referral_at"), F.col("referrer.timezone_homeclub"))
)

# Business Logic
valid_cond_1 = (
    (F.regexp_extract(F.col("rr.reward_value").cast("string"), r"(\d+)", 1).cast("int") > 0) & # 1. The reward value is greater than 0
    (F.col("urs.description").isin(["Berhasil", "berhasil", "BERHASIL"])) & # 2. The referral status is "Berhasil" (Successful)
    (F.col("pt.transaction_id").isNotNull()) & # 3. The referral has a transaction ID
    (F.upper(F.col("pt.transaction_status")) == "PAID") & # 4. The transaction status for the referral is "PAID"
    (F.upper(F.col("pt.transaction_type")) == "NEW") & # 5. The transaction type for the referral is "NEW"
    (F.col("transaction_at") > F.col("referral_at")) & # 6. The transaction occurred after the referral was created
    (F.month(F.col("transaction_at")) == F.month(F.col("referral_at"))) & # 7. The transaction happened in the same month as the referral creation
    (F.col("referrer.membership_expired_date") >= F.current_date()) & # 8. The referrer’s membership has not expired
    (F.col("referrer.is_deleted") == False) & # 9. The referrer’s account is not deleted
    (F.col("url.is_reward_granted") == True) # 10. The reward has been granted to the referee
)

valid_cond_2 = (
    F.col("urs.description").isin(["Menunggu", "menunggu", "Tidak Berhasil", "tidak berhasil"]) & #The referral status is either "Menunggu" (Pending) or "Tidak Berhasil" (Failed)
    (F.col("rr.reward_value").isNull()) #There is no reward value assigned
)

df_final = df_base.withColumn("is_business_logic_valid", 
    F.when(valid_cond_1 | valid_cond_2, True).otherwise(False)
)

# Select Output Columns
report_df = df_final.select(
    F.coalesce(F.col("url.id").cast("int"), F.lit(0)).alias("referral_details_id"),
    F.col("ur.referral_id"),
    F.col("ur.referral_source"),
    F.col("referral_source_category"),
    F.col("referral_at"),
    F.col("ur.referrer_id"),
    F.col("referrer.name").alias("referrer_name"),
    F.col("referrer.phone_number").alias("referrer_phone_number"),
    F.col("referrer.homeclub").alias("referrer_homeclub"),
    F.col("ur.referee_id"),
    F.col("ur.referee_name"),
    F.col("ur.referee_phone"),
    F.col("urs.description").alias("referral_status"),
    F.lit(30).alias("num_reward_days"), 
    F.col("pt.transaction_id"),
    F.col("pt.transaction_status"),
    F.col("transaction_at"),
    F.col("pt.transaction_location"),
    F.col("pt.transaction_type"),
    F.col("ur.updated_at"),
    F.col("url.created_at").alias("reward_granted_at"),
    F.col("is_business_logic_valid")
)

# Handling NULLS & DUPLICATES
report_df = report_df.na.drop(how="all")
report_df = report_df.dropDuplicates(["referral_id"])

# Convert every column to string, strip spaces, and definitively crush the word "null"
for c in report_df.columns:
    report_df = report_df.withColumn(c, F.col(c).cast("string"))
    report_df = report_df.withColumn(c, F.when(F.lower(F.trim(F.col(c))) == "null", "N/A").otherwise(F.col(c)))

report_df = report_df.na.fill("N/A")

# Generate and save the report
report_df.coalesce(1).write.option("nullValue", "N/A").csv("/app/output/final_report", header=True, mode="overwrite")
# Stop Spark session
spark.stop()