import pandas as pd
import re

# Load the data directly from the Excel file
file_path = "six_pillars_data.xlsx"
df = pd.read_excel(file_path)

# Isolate only the 6 dimension score columns
score_cols = [col for col in df.columns if 'Score_0_to_100' in col]

# Group by Company and Ticker, and calculate the mean ONLY for the score columns
avg_df = df.groupby(['Company_Name', 'Ticker'])[score_cols].mean().reset_index()

new_col_names = {}
for col in score_cols:
    clean_name = re.sub(r'DIMENSION_\d+_', '', col)
    clean_name = clean_name.replace('_Score_0_to_100', '')
    new_col_names[col] = f"Avg_{clean_name}"
    
avg_df.rename(columns=new_col_names, inplace=True)

# Save the resulting averages to a new Excel file
output_file = "average_pillars_scores.xlsx"
avg_df.to_excel(output_file, index=False)

# Print a preview of the results
print(f"Successfully processed {len(avg_df)} companies.")
print(avg_df.head())