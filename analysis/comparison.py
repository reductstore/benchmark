# %%
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")


# %%
# Define constants
label_system_one = "System 1 (ReductStore)"
label_system_two = "System 3 (TimescaleDB)"

HERE = os.path.abspath(os.path.dirname(__file__))
read_dir = f"{HERE}/../results"
save_dir = "graphs"
if not os.path.exists(save_dir):
    os.makedirs(save_dir)


# %%
# Import the data

df1 = pd.read_csv(os.path.join(read_dir, "SystemOne_read_write.csv"))
df1["system"] = label_system_one

df2 = pd.read_csv(os.path.join(read_dir, "SystemThree_read_write.csv"))
df2["system"] = label_system_two

df3 = pd.read_csv(os.path.join(read_dir, "SystemOne_batch_read.csv"))
df3["system"] = label_system_one

df4 = pd.read_csv(os.path.join(read_dir, "SystemThree_batch_read.csv"))
df4["system"] = label_system_two

df = pd.concat([df1, df2, df3, df4])

# %%
# Clean the data
df["blob_size"] = df["blob_size"].astype("int")
df["system"] = df["system"].astype("category")
df["write_time"] = df["write_time"].astype("float")
df["read_time"] = df["read_time"].astype("float")

df["total_time"] = df["write_time"] + df["read_time"]

df["total_time_ms"] = df["total_time"] * 1000
df["write_time_ms"] = df["write_time"] * 1000
df["read_time_ms"] = df["read_time"] * 1000


# %%
# Format the size in bytes to human readable format
def format_size_binary(size):
    size = int(size)
    if size >= 2**30:
        return f"{size // 2**30} GiB"
    elif size >= 2**20:
        return f"{size // 2**20} MiB"
    elif size >= 2**10:
        return f"{size // 2**10} KiB"
    else:
        return f"{size} B"


# %%
# Plot barplot and save the figure
def plot_barplot(df, x, y, hue, title, x_label, y_label, save_path):
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(
        ax=ax,
        data=df,
        x=x,
        y=y,
        hue=hue,
        estimator="median",
        errorbar=("pi", 50),
        palette=["#a5d8ff", "#ffd8a8"],
        width=0.8,
    )
    xlabels = [format_size_binary(item.get_text()) for item in ax.get_xticklabels()]
    ax.set_xticklabels(xlabels, rotation=90)
    ax.set_title(title)
    ax.set_ylabel(y_label)
    ax.set_xlabel(x_label)
    ax.legend(loc="best")
    for i in ax.containers:
        ax.bar_label(
            i,
            label_type="center",
            padding=0,
            rotation=0,
            fontsize=8,
            fmt="%.1f",
            color="black",
            weight="bold",
        )
    plt.show()
    if save_path is not None:
        fig.savefig(save_path, bbox_inches="tight", dpi=300)
    fig.clear()


# %%
# Result for individual write operations
plot_barplot(
    df=df[df["batch_size"] == 1],
    x="blob_size",
    y="write_time_ms",
    hue="system",
    title="Write Time vs Blob Size for Single Writes",
    x_label="Blob Size (bytes)",
    y_label="Write Time (ms)",
    save_path=os.path.join(save_dir, "single_write_time.png"),
)

# %%
# Result for individual read operations
plot_barplot(
    df=df[df["batch_size"] == 1],
    x="blob_size",
    y="read_time_ms",
    hue="system",
    title="Read Time vs Blob Size for Single Reads",
    x_label="Blob Size (bytes)",
    y_label="Read Time (ms)",
    save_path=os.path.join(save_dir, "single_read_time.png"),
)


# %%
# Result for batch read operations
plot_barplot(
    df=df[df["batch_size"] == 1_000],
    x="blob_size",
    y="read_time",
    hue="system",
    title="Read Time vs Blob Size for Batches of 1000 Blobs",
    x_label="Blob Size (bytes)",
    y_label="Read Time (s)",
    save_path=os.path.join(save_dir, "batch_read_time.png"),
)
# %%

# %%
# Result for for batch read operations below 64 KiB
plot_barplot(
    df=df[(df["batch_size"] == 1_000) & (df["blob_size"] < 64 * 2**10)],
    x="blob_size",
    y="read_time",
    hue="system",
    title="Read Time vs Blob Size for Batches of 1000 Blobs",
    x_label="Blob Size (bytes)",
    y_label="Read Time (s)",
    save_path=os.path.join(save_dir, "batch_read_time_small.png"),
)

# %%
# Result for for batch read operations above 64 KiB
plot_barplot(
    df=df[(df["batch_size"] == 1_000) & (df["blob_size"] >= 64 * 2**10)],
    x="blob_size",
    y="read_time",
    hue="system",
    title="Read Time vs Blob Size for Batches of 1000 Blobs",
    x_label="Blob Size (bytes)",
    y_label="Read Time (s)",
    save_path=os.path.join(save_dir, "batch_read_time_large.png"),
)

# %%
