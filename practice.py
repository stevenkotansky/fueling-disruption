import datetime
import numpy as np
import pandas as pd

print(pd.to_datetime("11/10/2018") > pd.to_datetime("11/11/2018"))
print(pd.to_datetime("11/10/2018") < pd.to_datetime("11/11/2018"))
print(pd.to_datetime("11/10/2018") == pd.to_datetime("11/10/2018"))
