from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import glob
import shutil

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

# 1. LOAD DATA, CAST TO STRING, AND RENAME PRECISELY
df_paid_transactions = spark.read.csv(f"{DATA_DIR}/paid_transactions.csv", **read_opts) \
    .withColumn("transaction_id", F.col("transaction_id").cast("string"))

df_referral_rewards = spark.read.csv(f"{DATA_DIR}/referral_rewards.csv", **read_opts) \
    .withColumn("reward_id", F.col("id").cast("string")).drop("id")

df_user_logs = spark.read.csv(f"{DATA_DIR}/user_logs.csv", **read_opts) \
    .withColumn("log_user_id", F.col("user_id").cast("string")).drop("user_id")

df_user_referral_logs = spark.read.csv(f"{DATA_DIR}/user_referral_logs.csv", **read_opts) \
    .withColumn("user_referral_id", F.col("user_referral_id").cast("string"))

df_user_referral_statuses = spark.read.csv(f"{DATA_DIR}/user_referral_statuses.csv", **read_opts) \
    .withColumn("status_id", F.col("id").cast("string")).drop("id")

df_user_referrals = spark.read.csv(f"{DATA_DIR}/user_referrals.csv", **read_opts) \
    .withColumn("referral_id", F.col("referral_id").cast("string")) \
    .withColumn("referrer_id", F.col("referrer_id").cast("string")) \
    .withColumn("referee_id", F.col("referee_id").cast("string")) \
    .withColumn("transaction_id", F.col("transaction_id").cast("string")) \
    .withColumn("user_referral_status_id", F.col("user_referral_status_id").cast("string")) \
    .withColumn("referral_reward_id", F.col("referral_reward_id").cast("string"))

df_lead_log = spark.read.csv(f"{DATA_DIR}/lead_log.csv", **read_opts) \
    .withColumn("lead_log_id", F.col("lead_id").cast("string")).drop("lead_id")

# 2. Data cleaning & Formatting
def apply_initcap(df, exclude_cols=[]):
    for col_name, dtype in df.dtypes:
        if dtype == 'string' and 'club' not in col_name.lower() and col_name not in exclude_cols:
            df = df.withColumn(col_name, F.initcap(F.col(col_name)))
    return df

df_user_logs = apply_initcap(df_user_logs)
df_user_referrals = apply_initcap(df_user_referrals, exclude_cols=['referral_id', 'referrer_id', 'referee_id', 'transaction_id'])

# 3. JOINING
df_base = df_user_referrals.alias("ur") \
    .join(df_user_referral_logs.alias("url"), F.col("ur.referral_id") == F.col("url.user_referral_id"), "left") \
    .join(df_user_referral_statuses.alias("urs"), F.col("ur.user_referral_status_id") == F.col("urs.status_id"), "left") \
    .join(df_referral_rewards.alias("rr"), F.col("ur.referral_reward_id") == F.col("rr.reward_id"), "left") \
    .join(df_paid_transactions.alias("pt"), "transaction_id", "left") \
    .join(df_user_logs.alias("referrer"), F.col("ur.referrer_id") == F.col("referrer.log_user_id"), "left") \
    .join(df_lead_log.alias("lead"), (F.col("ur.referral_source") == 'Lead') & (F.col("ur.referee_id") == F.col("lead.lead_log_id")), "left")

# 4. Source Category Logic
df_base = df_base.withColumn("referral_source_category", 
    F.when(F.col("ur.referral_source") == 'User Sign Up', 'Online')
     .when(F.col("ur.referral_source") == 'Draft Transaction', 'Offline')
     .when(F.col("ur.referral_source") == 'Lead', F.col("lead.source_category"))
     .otherwise('Unknown')
)

# 5. Time Adjustment
df_base = df_base.withColumn("transaction_at_local", F.from_utc_timestamp(F.col("pt.transaction_at"), F.col("pt.timezone_transaction"))) \
                 .withColumn("referral_at_local", F.from_utc_timestamp(F.col("ur.referral_at"), F.col("referrer.timezone_homeclub")))

# 6. BUSINESS LOGIC
reward_val = F.regexp_extract(F.col("rr.reward_value").cast("string"), r"(\d+)", 1).cast("int")

valid_cond_1 = (
    (reward_val > 0) & 
    (F.col("urs.description") == "Berhasil") & 
    (F.col("transaction_id").isNotNull()) & 
    (F.upper(F.col("pt.transaction_status")) == "PAID") & 
    (F.upper(F.col("pt.transaction_type")) == "NEW") & 
    (F.col("transaction_at_local") > F.col("referral_at_local")) & 
    (F.month(F.col("transaction_at_local")) == F.month(F.col("referral_at_local"))) & 
    (F.col("referrer.membership_expired_date") > F.col("ur.referral_at")) & 
    (F.col("referrer.is_deleted") == False)
)

valid_cond_2 = (
    F.col("urs.description").isin(["Menunggu", "Tidak Berhasil"]) & 
    (reward_val.isNull() | (reward_val == 0))
)

df_final = df_base.withColumn("is_business_logic_valid", 
    F.when(valid_cond_1 | valid_cond_2, True).otherwise(False)
)

# 7. FORMATTING & DEDUPLICATION
report_df = df_final.select(
    F.col("ur.referral_id"),
    F.col("ur.referral_source"),
    F.col("referral_source_category"),
    F.col("referral_at_local").alias("referral_at"),
    F.col("ur.referrer_id"),
    F.col("referrer.name").alias("referrer_name"),
    F.col("referrer.phone_number").alias("referrer_phone_number"),
    F.col("referrer.homeclub").alias("referrer_homeclub"),
    F.col("ur.referee_id"),
    F.col("ur.referee_name"),
    F.col("ur.referee_phone"),
    F.col("urs.description").alias("referral_status"),
    reward_val.alias("num_reward_days"), 
    F.col("transaction_id"),
    F.col("pt.transaction_status"),
    F.col("transaction_at_local").alias("transaction_at"),
    F.col("pt.transaction_location"),
    F.col("pt.transaction_type"),
    F.col("ur.updated_at"),
    F.col("url.created_at").alias("reward_granted_at"),
    F.col("is_business_logic_valid")
).dropDuplicates(["referral_id"])

report_df = report_df.na.drop(how="all")

# 8. CONSECUTIVE ID
w = Window.orderBy("referral_id")
report_df = report_df.withColumn("referral_details_id", F.row_number().over(w) + 100)
cols = ["referral_details_id"] + [c for c in report_df.columns if c != "referral_details_id"]
report_df = report_df.select(cols)

# Final N/A Cleanup
for c in report_df.columns:
    # First, ensure everything is a string
    report_df = report_df.withColumn(c, F.col(c).cast("string"))
    
    # Catch both true NULLs AND the literal text word "null"
    report_df = report_df.withColumn(c, 
        F.when(F.col(c).isNull(), "N/A")
         .when(F.lower(F.trim(F.col(c))) == "null", "N/A")
         .otherwise(F.col(c))
    )
    
# ============================================================
# 9. OUTPUT FINALIZATION (RENAME LOGIC)
# ============================================================
temp_dir = "/app/output/temp_spark_out"
final_file_path = "/app/output/referral_report.csv"

report_df.coalesce(1).write.option("header", "true").option("nullValue", "N/A").csv(temp_dir, mode="overwrite")

try:
    csv_file = glob.glob(f"{temp_dir}/*.csv")[0]
    shutil.move(csv_file, final_file_path)
    shutil.rmtree(temp_dir)
    print(f"File successfully created: {final_file_path}")
except IndexError:
    print("Error: No CSV file found.")

spark.stop()