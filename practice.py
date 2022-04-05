import pandas as pd

# Connect to and format COVID case data
df_covid = pd.read_csv("ChicagoCOVID.csv")
df_covid = df_covid.rename(columns={
    "Week Start": "Date", "Cases - Weekly": "Cases"})
df_covid = df_covid[df_covid["Cases"] > 0]
df_covid["Date"] = pd.to_datetime(df_covid["Date"])
df_covid = df_covid.drop(columns=["ZIP Code", "Week Number", "Week End", "Cases - Cumulative", "Case Rate - Weekly", "Case Rate - Cumulative", "Tests - Weekly", "Tests - Cumulative", "Test Rate - Weekly", "Test Rate - Cumulative",
                         "Percent Tested Positive - Weekly", "Percent Tested Positive - Cumulative", "Deaths - Weekly", "Deaths - Cumulative", "Death Rate - Weekly", "Death Rate - Cumulative", "Population", "Row ID", "ZIP Code Location"])

# Aggregate COVID cases to be daily
df_covid = df_covid.groupby(['Date'], as_index=False)['Cases'].sum()

#Shift days to allign with other datasets
df_cases = df_covid.set_index("Date")
df_cases = df_cases.shift(freq="1D")
print(df_cases)