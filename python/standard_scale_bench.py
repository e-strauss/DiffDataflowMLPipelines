import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import time
import matplotlib.pyplot as plt

if __name__ == '__main__':
    basesize = 1000000
    update_sizes = np.array([1, 10, 100, 1000, 10000, 100000, 1000000])

    execution_times = []

    rust_update_only = np.array([0.00145, 0.00145, 0.00222, 0.0105, 0.096, 1.0, 10.27])
    rust_total = np.array([10.28, 10.28, 10.25, 10.27, 10.29, 11.40, 20.26])

    for us in update_sizes:
        df = pd.DataFrame({'values': np.random.rand(basesize + us)})
        scaler = StandardScaler()

        start_time = time.time()
        scaled_df = scaler.fit_transform(df)
        end_time = time.time()

        execution_time = end_time - start_time
        execution_times.append(execution_time)
        print(f"time for {us}: {execution_time:.6f} seconds")

    execution_times = np.array(execution_times)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

    ax1.plot(update_sizes, execution_times + execution_times[0], marker='o', label="sklearn", color="b")
    ax1.plot(update_sizes, rust_total, marker='o', label="ours", color="orange")
    ax1.set_xlabel('Update Size')
    ax1.set_ylabel('Total Execution Time (seconds)')
    ax1.set_title('Total Execution Time vs Update Size')
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.grid(True)
    ax1.legend()

    ax2.plot(update_sizes, execution_times, marker='o', label="sklearn", color="b")
    #ax2.fill_between(update_sizes, execution_times - execution_times*0.2, execution_times + execution_times*0.2, alpha=0.2)

    ax2.plot(update_sizes, rust_update_only, marker='o', label="ours", color="orange")
    ax2.plot(update_sizes, rust_update_only + rust_update_only * 1000, marker="o", linestyle='dashed', alpha=0.7, color="orange")


    ax2.set_xlabel('Update Size')
    ax2.set_ylabel('Update Execution Time (seconds)')
    ax2.set_title('Update Execution Time vs Update Size')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.grid(True)
    ax2.legend()



    plt.tight_layout()
    plt.show()