import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from scipy.spatial.distance import cdist
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense, Dropout, Input, RepeatVector, TimeDistributed
import tensorflow as tf
from sklearn.metrics import confusion_matrix, classification_report

# Create a connection to the SQLite database
conn = sqlite3.connect('cgm_light.db')
query = "SELECT * FROM cgm_data"
df = pd.read_sql(query, conn)
conn.close()

# Convert datetime and sort
df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values(['series_id', 'datetime'])

# Focus on one series for the example
series_id = 2
series_df = df[df['series_id'] == series_id].copy()
series_df.set_index('datetime', inplace=True)

# Calculate glucose rate of change (mg/dL per minute)
series_df['glucose_diff'] = series_df['blood_glucose'].diff()
series_df['minutes_diff'] = series_df.index.to_series().diff().dt.total_seconds() / 60
series_df['rate_of_change'] = series_df['glucose_diff'] / series_df['minutes_diff']
series_df['roc_diff'] = series_df['rate_of_change'].diff()
series_df['acceleration'] = series_df['roc_diff'] / series_df['minutes_diff']

# Remove first row with NaN diff
series_df = series_df.dropna()  # Remove first row with NaN diff

print("Data overview with rate of change:")
print(series_df.head())

# 1. Statistical Approach: Z-Score Method
def z_score_anomalies(df, threshold=3.0):
    """Detect anomalies in rate of change and acceleration using Z-scores"""
    # Z-score for rate of change
    df['roc_zscore'] = np.abs(stats.zscore(df['rate_of_change']))
    # Z-score for acceleration
    df['acc_zscore'] = np.abs(stats.zscore(df['acceleration']))

    # Mark as anomaly if both Z-scores exceeds threshold
    anomaly_mask = (df['acc_zscore'] > threshold) & (df['roc_zscore'] > threshold)

    return anomaly_mask

# 2. Isolation Forest
def isolation_forest_anomalies(df, contamination=0.05):
    """Detect anomalies using Isolation Forest"""
    features = df[['rate_of_change', 'acceleration']].values

    # Fit Isolation Forest
    model = IsolationForest(random_state=42, contamination=contamination)
    preds = model.fit_predict(features)

    # -1 for anomalies, 1 for normal
    return pd.Series(preds == -1, index=df.index)

# DBSCAN Clustering
def dbscan_anomalies(df, eps=0.5, min_samples=5):
    """Detect anomalies using DBSCAN clustering"""
    # Standardize features
    scaler = StandardScaler()
    features = scaler.fit_transform(df[['rate_of_change', 'acceleration']])

    # Fit DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    clusters = dbscan.fit_predict(features)

    # -1 indicates noise points (anomalies)
    return pd.Series(clusters == -1, index=df.index)

# k-means clustering
def kmeans_anomalies(df, n_clusters=3, distance_threshold=3.5):
    scaler = StandardScaler()
    features = scaler.fit_transform(df[['rate_of_change', 'acceleration']].values)


    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(features)
    centroids = kmeans.cluster_centers_ # get center of clusters

    # get distance to each point from nearest centroid
    distances = cdist(features, centroids)
    min_distances = np.min(distances, axis=1)

    # Set threshold for anomaly detection
    # Points with distance > mean + threshold * std are considered anomalies
    threshold = np.mean(min_distances) + distance_threshold * np.std(min_distances)
    anomaly_mask = min_distances > threshold

    # Plot clusters and distances for visualization
    # plotting code partially developed with help from Claude
    plt.figure(figsize=(16, 6))
    
    # Plot 1: Clusters
    plt.subplot(1, 2, 1)
    for i in range(n_clusters):
        cluster_points = features[cluster_labels == i]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], 
                   label=f'Cluster {i}', alpha=0.7)
    
    plt.scatter(centroids[:, 0], centroids[:, 1], 
               marker='x', s=100, linewidths=3, color='black', 
               label='Centroids')
    
    plt.scatter(features[anomaly_mask, 0], features[anomaly_mask, 1],
               s=100, edgecolors='red', facecolors='none', linewidths=2,
               label='Anomalies')
    
    plt.title('K-means Clusters with Anomalies')
    plt.xlabel('Standardized Rate of Change')
    plt.ylabel('Standardized Acceleration')
    plt.legend()
    plt.grid(True)
    
    # Plot 2: Distance distribution
    plt.subplot(1, 2, 2)
    plt.hist(min_distances, bins=30, alpha=0.7)
    plt.axvline(x=threshold, color='red', linestyle='--', 
               label=f'Threshold ({threshold:.2f})')
    plt.title('Distance to Nearest Centroid')
    plt.xlabel('Distance')
    plt.ylabel('Frequency')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('figures/kmeans_clusters_and_distances.png')
    plt.close()
    
    # Return anomaly mask
    return pd.Series(anomaly_mask, index=df.index)


# Combined Approach: Majority voting
def combined_anomaly_detection(list_of_masks):
    # get all of the masks combined
    anomaly_votes = list_of_masks[0].astype(int)
    for mask in list_of_masks[1:]:
        anomaly_votes = anomaly_votes + mask.astype(int)

    # take the majority vote of the masks
    return anomaly_votes >= 3

print("\nApplying anomaly detection methods...")

# function to plot some of the anomalies
# Subplot plotting partially assisted using Claude
def plot_anomalies(df, anomaly_mask, title, filename, context_minutes=60):
    # Count anomalies
    n_anomalies = anomaly_mask.sum()
    print(f"Found {n_anomalies} anomalies using {title}")

    if n_anomalies == 0:
        # If no anomalies, just show the overall plot
        plt.figure(figsize=(14, 7))
        plt.plot(df.index, df['blood_glucose'], label='Blood Glucose', color='blue')
        plt.title(f"{title} - No Anomalies Detected")
        plt.xlabel('Time')
        plt.ylabel('Blood Glucose (mg/dL)')
        plt.grid(True)
        plt.savefig(filename)
        plt.close()
        return 0

    # First create an overview plot with all data
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['blood_glucose'], label='Blood Glucose', color='blue', alpha=0.5)
    plt.scatter(df[anomaly_mask].index, df[anomaly_mask]['blood_glucose'],
                color='red', label='Anomalies', s=80, zorder=5)
    plt.title(f"{title} - Overview")
    plt.xlabel('Time')
    plt.ylabel('Blood Glucose (mg/dL)')
    plt.legend()
    plt.grid(True)
    plt.savefig(f"{filename.split('.')[0]}_overview.png")
    plt.close()

    # Now create focused plots around each anomaly
    # Limit to max 10 subplots to avoid too many plots
    anomaly_indices = np.where(anomaly_mask)[0]
    max_plots = min(10, len(anomaly_indices))

    # If there are too many anomalies, sample evenly across the range
    if len(anomaly_indices) > max_plots:
        step = len(anomaly_indices) // max_plots
        anomaly_indices = anomaly_indices[::step][:max_plots]

    # Create a multi-page figure with subplots
    fig, axes = plt.subplots(len(anomaly_indices), 1, figsize=(12, 4*len(anomaly_indices)))

    # Handle case with only one subplot
    if len(anomaly_indices) == 1:
        axes = [axes]

    for i, idx in enumerate(anomaly_indices):
        anomaly_time = df.index[idx]

        # Define time window around anomaly
        start_time = anomaly_time - pd.Timedelta(minutes=context_minutes)
        end_time = anomaly_time + pd.Timedelta(minutes=context_minutes)

        # Get data in time window
        mask = (df.index >= start_time) & (df.index <= end_time)
        window_df = df[mask]

        # Plot regular data in window
        axes[i].plot(window_df.index, window_df['blood_glucose'], 'b-', label='Blood Glucose')

        # Highlight the anomaly
        axes[i].scatter([anomaly_time], [df.iloc[idx]['blood_glucose']],
                       color='red', s=100, zorder=5, label='Anomaly')

        # Add rate of change annotation
        roc = df.iloc[idx]['rate_of_change'] if 'rate_of_change' in df.columns else None
        if roc is not None:
            axes[i].annotate(f"RoC: {roc:.2f} mg/dL/min",
                           (anomaly_time, df.iloc[idx]['blood_glucose']),
                           xytext=(10, -30), textcoords='offset points',
                           arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"))

        # Add value before and after anomaly
        if idx > 0:
            prev_time = df.index[idx-1]
            prev_val = df.iloc[idx-1]['blood_glucose']
            time_diff = (anomaly_time - prev_time).total_seconds() / 60  # in minutes
            axes[i].annotate(f"{prev_val:.1f} mg/dL\n{time_diff:.1f}min before",
                           (prev_time, prev_val),
                           xytext=(-40, 20), textcoords='offset points',
                           arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=-.2"))

        if idx < len(df) - 1:
            next_time = df.index[idx+1]
            next_val = df.iloc[idx+1]['blood_glucose']
            time_diff = (next_time - anomaly_time).total_seconds() / 60  # in minutes
            if next_time <= end_time:  # Only show if within our window
                axes[i].annotate(f"{next_val:.1f} mg/dL\n{time_diff:.1f}min after",
                               (next_time, next_val),
                               xytext=(40, 20), textcoords='offset points',
                               arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"))

        # Format the subplot
        axes[i].set_title(f"Anomaly at {anomaly_time.strftime('%Y-%m-%d %H:%M')}")
        axes[i].set_xlabel('Time')
        axes[i].set_ylabel('Blood Glucose (mg/dL)')
        axes[i].grid(True)

        # Create a reasonable y-axis range (±30% from anomaly value)
        anomaly_val = df.iloc[idx]['blood_glucose']
        y_range = max(30, anomaly_val * 0.3)  # at least 30 mg/dL range
        axes[i].set_ylim([max(40, anomaly_val - y_range), anomaly_val + y_range])

        # Add legend to first subplot only
        if i == 0:
            axes[i].legend()

    plt.tight_layout()
    plt.savefig(f"{filename.split('.')[0]}_detailed.png")
    plt.close()

    # If we have more anomalies than we showed in detailed plots, mention this
    if len(np.where(anomaly_mask)[0]) > max_plots:
        print(f"Note: Showing detailed plots for {max_plots} of {n_anomalies} anomalies")

    return n_anomalies

# Z-Score method
anomalies_zscore = z_score_anomalies(series_df)
n_zscore = plot_anomalies(
    series_df,
    anomalies_zscore,
    "Anomalies Detected by Z-Score Method",
    "figures/zscore_anomalies.png"
)

# Isolation Forest
anomalies_iforest = isolation_forest_anomalies(series_df)
n_iforest = plot_anomalies(
    series_df,
    anomalies_iforest,
    "Anomalies Detected by Isolation Forest",
    "figures/iforest_anomalies.png"
)

# DBSCAN
anomalies_dbscan = dbscan_anomalies(series_df, eps=1.0, min_samples=5)
n_dbscan = plot_anomalies(
    series_df,
    anomalies_dbscan,
    "Anomalies Detected by DBSCAN",
    "figures/dbscan_anomalies.png"
)

anomalies_kmeans = kmeans_anomalies(series_df)
n_kmeans = plot_anomalies(
    series_df,
    anomalies_kmeans,
    "Anomalies Detected by k-means",
    "figures/kmeans_anomalies.png"
)

# Combined approach
anomalies_combined = combined_anomaly_detection([anomalies_dbscan, anomalies_iforest, anomalies_kmeans, anomalies_zscore])
n_combined = plot_anomalies(
    series_df,
    anomalies_combined,
    "Anomalies Detected by Combined Methods",
    "figures/combined_anomalies.png"
)

# Create a summary table
methods = ['Z-Score', 'Isolation Forest', 'DBSCAN', 'k-means', 'Combined (Majority Vote)']
anomaly_counts = [n_zscore, n_iforest, n_dbscan, n_kmeans, n_combined]
summary_df = pd.DataFrame({
    'Method': methods,
    'Anomalies Detected': anomaly_counts,
    'Percentage': [count / len(series_df) * 100 for count in anomaly_counts]
})

print("\nSummary of anomaly detection methods:")
print(summary_df)