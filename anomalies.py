import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from loglizer.models import IsolationForest
from loglizer import preprocessing

CLEAN_LOGS_DIR = Path("cleanlogs")
ANOMALY_OUTPUT = "anomalies.txt"
TOP_K = 50  # Number of top anomalies to save (change as needed)

# Collect all log lines from all .log files
log_lines = []
file_indices = []
file_names = []

log_files = list(CLEAN_LOGS_DIR.glob("*.log"))
if not log_files:
    print(f"No .log files found in {CLEAN_LOGS_DIR.resolve()}. Exiting.")
    exit(1)

for file in tqdm(log_files, desc="Loading logs"):
    with open(file, 'r', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if line:
                log_lines.append(line)
                file_indices.append(len(file_names))
    file_names.append(file.name)

print(f"Loaded {len(log_lines)} log lines from {len(file_names)} files.")

if not log_lines:
    print("No log lines found. Exiting.")
    exit(1)

# Prepare dataframe (required by Loglizer)
df = pd.DataFrame({'Content': log_lines})

# TF-IDF feature extraction
vectorizer = preprocessing.TFIDFVectorizer()
X = vectorizer.fit_transform(df['Content'])

# Fit Isolation Forest for anomaly detection
model = IsolationForest(contamination=0.01, random_state=42)
model.fit(X)
anomaly_scores = model.decision_function(X)
outliers = model.predict(X) == -1

# Get top anomalies by score (lowest scores are most anomalous)
top_idx = anomaly_scores.argsort()[:min(TOP_K, len(log_lines))]

with open(ANOMALY_OUTPUT, "w", encoding="utf-8") as out:
    out.write(f"Top {len(top_idx)} anomalies found in log data:\n")
    for idx in top_idx:
        line_text = log_lines[idx]
        out.write(f"Score: {anomaly_scores[idx]:.5f} | Line: {line_text}\n")
        out.write("-" * 80 + "\n")

print(f"Anomaly detection complete. Top {len(top_idx)} anomalies written to {ANOMALY_OUTPUT}")
print(f"Total anomalies flagged: {outliers.sum()} of {len(log_lines)} log lines.")