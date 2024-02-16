import pandas as pd
from alphaflow.apis import fetch_fear_greed_index

fear_greed_index = fetch_fear_greed_index()
print(fear_greed_index)