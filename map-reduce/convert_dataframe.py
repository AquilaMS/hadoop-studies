import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv('raw_titles.csv')
    final_df = df.to_csv('df_netflix.csv', sep='|')