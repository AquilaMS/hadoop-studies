import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv('tmbdb_updated.csv')
    final_df = df.to_csv('tmdb.csv', sep='|', index=False)
    print(df.columns)