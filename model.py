import pandas as pd
import numpy as np
import sklearn
from sklearn.linear_model import LinearRegression

df = pd.read_csv("ModelCapstoneData.csv")
reg = linear_model.LinearRegression()
reg.fit(df[['B', 'C']], df['A'])
