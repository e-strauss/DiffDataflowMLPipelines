import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import time
import sys

data = pd.read_csv("data/5050_split.csv")

X = data.drop('Diabetes_binary', axis=1)
frac = float(sys.argv[1])
num_init = int(len(X)*frac)

X_init = X[:num_init]
X_updates = X[num_init:]
pipeline = Pipeline([
    ('scaling', StandardScaler()),  # Apply scaling
])

start1 = time.perf_counter()
pipeline.fit(X_init)
out = pipeline.transform(X_init)
end1 = time.perf_counter()
print(f"Time taken for init: {int((end1 - start1) * 1_000_000)}")

for i in range(len(X_updates)):
    start_tmp = time.perf_counter()
    X_init = pd.concat((X_init, X_updates[i:i+1]))
    pipeline.fit(X_init)
    out = pipeline.transform(X_init)
    end_tmp = time.perf_counter()
    print(f"{int((end_tmp - start_tmp) * 1_000_000)}")
end2 = time.perf_counter()
print("#updates: {}".format(len(X_updates)))
print(f"Time taken in total: {int((end2 - start1) * 1_000_000)} qs")
