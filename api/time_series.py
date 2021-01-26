from tslearn.clustering import TimeSeriesKMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from matplotlib import pyplot
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np

def plot_cluster_tickers(current_cluster, to):
    fig, ax = plt.subplots(
        int(np.ceil(current_cluster.shape[0]/4)),
        4,
        figsize=(15, 3*int(np.ceil(current_cluster.shape[0]/4)))
    )
    fig.autofmt_xdate(rotation=45)
    ax = ax.reshape(-1)

    for index, (_, row) in enumerate(current_cluster.iterrows()):
        ax[index].plot(row.iloc[1:to])
        ax[index].set_title(f"{row.eventId}")
        plt.xticks(rotation=45)
        if index==11:
            break

    plt.tight_layout()
    plt.show()

def find_kmeans(df_scaled, metric, clasters):
    distortions = []
    silhouette = []
    daviesbouldin = []
    K = range(1, clasters)
    for k in tqdm(K):
        kmeanModel = TimeSeriesKMeans(n_clusters=k, metric=metric, n_jobs=20, max_iter=10)
        #kmeanModel = TimeSeriesKMeans(n_clusters=k, metric="euclidean", n_jobs=6, max_iter=10)
        kmeanModel.fit(df_scaled)
        distortions.append(kmeanModel.inertia_)
        if k > 1:
            silhouette.append(silhouette_score(df_scaled, kmeanModel.labels_))
            daviesbouldin.append(davies_bouldin_score(df_scaled, kmeanModel.labels_))

    plt.figure(figsize=(10,4))
    plt.plot(K, distortions, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Distortion')
    plt.title('Elbow Method')
    plt.show()

    plt.figure(figsize=(10,4))
    plt.plot(K[1:], silhouette, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Silhouette score')
    plt.title('Silhouette')
    plt.show()

    plt.figure(figsize=(10,4))
    plt.plot(K[1:], daviesbouldin, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Davies-Bouldin score')
    plt.title('Davies-Bouldin')
    plt.show()

def calc_kmeans(df_scaled, metric, n_clusters, name):
    file_name='models/ts_{}_{}.pickle'.format(name, n_clusters)
    if not path.exists(file_name):
        ts_kmeans = TimeSeriesKMeans(n_clusters=n_clusters, metric=metric, n_jobs=20, max_iter=10)
        ts_kmeans.fit(df_scaled)
        with open(file_name, 'wb') as f:
            pickle.dump(ts_kmeans, f)
    else:
        ts_kmeans=pickle.load(open(file_name, 'rb'))

    for cluster_number in range(n_clusters):
        plt.plot(ts_kmeans.cluster_centers_[cluster_number, :, 0].T, label=cluster_number)
    plt.title("Cluster centroids")
    plt.legend()
    plt.show()
    return ts_kmeans