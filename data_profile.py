import pandas as pd
import os

# Define the data directory
DATA_DIR = './data/'
OUTPUT_DIR = './output/'

def profile_data(file_name):
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(file_path)
    
    profiling_data = []
    
    for col in df.columns:
        col_data = df[col]
        
        # Calculate basic metrics
        null_count = col_data.isnull().sum()
        total_count = len(col_data)
        pct_populated = ((total_count - null_count) / total_count) * 100
        distinct_count = col_data.nunique()
        
        try:
            min_val = col_data.dropna().min() if not col_data.isnull().all() else None
            max_val = col_data.dropna().max() if not col_data.isnull().all() else None
        except TypeError:
            # If it can't compare mixed types, safely skip
            min_val = "Mixed Types"
            max_val = "Mixed Types"
        
        max_length = col_data.astype(str).str.len().max() if col_data.dtype == 'object' else None
        
        profiling_data.append({
            'Column Name': col,
            'Data Type': str(col_data.dtype),
            'Defined Length': max_length, # Approximated via max actual length
            'Nulls Allowed': 'Yes',
            'Null Count': null_count,
            'Percentage Populated': f"{pct_populated:.2f}%",
            'Distinct Value Count': distinct_count,
            'Minimum Value': min_val,
            'Maximum Value': max_val,
            'Max Actual Length': max_length
        })
        
    report_df = pd.DataFrame(profiling_data)
    report_name = file_name.replace('.csv', '_profile.csv')
    report_df.to_csv(os.path.join(OUTPUT_DIR, report_name), index=False)
    print(f"Profiling complete for {file_name}. Saved to {report_name}")

if __name__ == "__main__":
    files = [
        "paid_transactions.csv", "referral_rewards.csv", "user_logs.csv", 
        "user_referral_logs.csv", "user_referral_statuses.csv", 
        "user_referrals.csv", "lead_log.csv"
    ]
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    for f in files:
        profile_data(f)