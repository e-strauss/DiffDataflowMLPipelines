import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import time

data = pd.read_csv("data/5050_split.csv")

X = data.drop('Diabetes_binary', axis=1)
num_init = int(len(X)*0.99)
X_init = X[:num_init]
X_updates = X[num_init:]
pipeline = Pipeline([
    ('scaling', StandardScaler()),  # Apply scaling
])

start1 = time.perf_counter()
pipeline.fit(X_init)
out = pipeline.transform(X_init)
end1 = time.perf_counter()
print(f"Time taken for init: {end1 - start1:.6f} seconds")

for i in range(len(X_updates)):
    start_tmp = time.perf_counter()
    X_init = pd.concat((X_init, X_updates[i:i+1]))
    pipeline.fit(X_init)
    out = pipeline.transform(X_init)
    end_tmp = time.perf_counter()
    print(f"Time taken for update: {end_tmp - start_tmp:.6f} seconds")
end2 = time.perf_counter()
print("#updates: {}".format(len(X_updates)))
print(f"Time taken in total: {end2 - start1:.6f} seconds")
