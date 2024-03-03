# Distributed Data Processing with Apache Airflow, MongoDB, and SparkSQL

## Approach / Tentative Outline

**Proposal**: Develop a data pipeline in Airflow that can be used to build an application to work as a localized social media platform for small groups of friends. The application will allow users to connect their Spotify accounts and share their listening history. We perform analytics on the data to determine who listened to what the most, and who listened to the same songs the most, and other analytics based on the original datasets.

**Tech Stack:** Apache Airflow, MongoDB, SparkSQL, Google Cloud Platform, Spotify API, Python, PyMongo, Spotipy, SparkML, MongoDB Atlas, Flask, Jinja2.

Use Airflow to orchestrate the following tasks via CRON jobs or other scheduling methods:

<p align="center">
  <kbd><img src="images/entire_worflow.png" width=400></img></kbd>
</p>
<p align="center"><em>Figure 1: Workflow Graph from the Airflow UI on Google Composer</a></em></p>

1. Use the script to connect to the Spotify API and collect data user's in a friend group.
2. Load the data into Google Cloud Storage (GCS)
3. Import the data into MongoDB in a collection, we can use the `pymongo` library to interact with MongoDB.
4. Create new datasets from analytical data such user's favorite artists, songs, etc. based on the original datasets.
5. Store the aggregates in a separate collection in MongoDB (`pymongo`) on MongoDB Atlas (cloud-based MongoDB service).
6. Query the data using MongoDB / NoSQL queries.
7. Create Dataframes from the data from MongoDB Atlas.
8. Run SparkSQL queries (Dataproc Cluster) over the data frames.
9. (Optional) Use a recommendation system to recommend songs to users based on their group listening history, via SparkSQL / SparkML.
10. Refresh a dashboard with the latest data from MongoDB Atlas, using Flask and Jinja2 (Future)


## Prerequisites: Connect to Spotify API for data collection

There is a library in Python called [Spotipy](https://spotipy.readthedocs.io/en/2.22.1/) that can be used to connect to the Spotify API. This is a lightweight Python library for the Spotify Web API. It includes support for all the features of the Spotify Web API including access to all end points, and support for user authorization.

Spohttps://spotipy.readthedocs.io/en/2.22.1/

#### Security Considerations

- **Securely store credentials** (client ID, client secret, access, and refresh tokens) to prevent unauthorized access.
- **Use HTTPS** for redirect URIs to protect sensitive data during the OAuth flow.
- **Implement CSRF protection** by validating a state parameter during the authentication callback to prevent CSRF attacks.
- **Sanitize user input** and external API data to avoid XSS and injection attacks.
- **Handle token expiry** by implementing a system to refresh tokens automatically without user intervention.
- **Implement error handling** for expired tokens and API errors, including Spotify’s rate limiting.

## Step 1: Use the script to connect to the Spotify API and collect data

<p align="center">
  <kbd><img src="images/dag_sample.png" width=500></img></kbd>
</p>
<p align="center"><em>Figure 2: Glimpse of the DAG</a></em></p>

The access token is a string which contains the credentials and permissions that can be used to access a given resource (e.g artists, albums or tracks) or user's data (e.g your profile or your playlists). Your application requests authorization to access service resources from the user and the service then issues access tokens to the application. Note that the access token is valid for 1 hour (3600 seconds). After that time, the token expires and you need to request a new one. If we use the `spotipy` library, it will handle the token refresh for us. After 60 days the refresh token will expire and the user will need to re-authenticate. There is a `prompt_for_user_token` method in the `spotipy` library that can be used to get the access token. 

If we want to avoid having to re-authenticate every 60 days, we can use the [Authorization Code Flow with Proof Key for Code Exchange (PKCE)](https://developer.spotify.com/documentation/web-api/tutorials/code-pkce-flow) to get a refresh token. This method is suitable for mobile and desktop applications. It is recommended for applications that cannot store the client secret securely. Or we can use the [Authorization Code Flow](https://developer.spotify.com/documentation/web-api/tutorials/code-flow) to get a refresh token. This method is suitable for web applications. It is recommended for applications that can store the client secret securely.

## Step 2: Set Up Google Cloud Composer Environment and MongoDB Cluster

After connecting to the Spotify API, we created a Google Cloud account, set up an environment in Google Cloud Composer, and defined the necessary libraries. Additionally, we established a MongoDB Atlas cluster to store the data retrieved from Spotify, preparing for a script-based data upload.



## Step 3: Implement MongoDB Script Workflows in Google Cloud Composer

<p align="center">
  <kbd><img src="images/script_workflows.png" width=500></img></kbd>
</p>
<p align="center"><em>Figure 3: MongoDB Script Workflow</a></em></p>

Then, we uploaded the scripts in Figure 3 to Google Cloud Composer to set up the automated data pipeline. The process starts with the `data_fetcher.py` script, which is the starting point for data retrieval and then triggering other scripts for specific tasks after completion. The whole pipeline is as follows:
1. The Data Fetcher script (`data_fetcher.py`) is first, connecting to Spotify's API to pull user data, ensuring that the data is fetched accurately and efficiently, handling any API limitations or data paging requirements. This also includes account access approval for all members in group.
2. Once the Spotify data is retrieved, the workflow advances to processing with `recently_played_json_converter.py` and `top_tracks_json_converter.py`. These scripts convert song durations to minutes:seconds, align timestamps to Pacific Time, and reformat the data for ease of analysis, discarding irrelevant market data. 
3. The `data_upload.py` script connects and uploads the data to a MongoDB Atlas cluster, forming two main collections: rawdata_recentlyplayed and rawdata_toptracks.
4. Further refinement is done by `create_collections_recently_played.py` and  `create_collections_top_tracks.py`, establishing specialized MongoDB collections to facilitate various analyses. These schemas capture song metadata, track details, album information, artist data, and images, with specific collections like **t50_artists**, **t50_Images**, and **t50_Tracks** for top tracks' data.  

This streamlined workflow in Google Cloud Composer ensures a consistent, automated process for data handling, setting the stage for advanced analytics.

<p align="center">
  <kbd><img src="images/composer.png" width=500></img></kbd>
</p>
<p align="center"><em>Figure 4: Screenshot of the Google Cloud Composer</a></em></p>

## Step 4: Integrate PySpark and MongoDB for Data Visualization

<p align="center">
  <kbd><img src="images/dag_grid.png" width=500></img></kbd>
</p>
<p align="center"><em>Figure 5: Grid View of the DAG runs</a></em></p>

1. **Upload MongoDB Collections to Google Cloud Storage (GCS)**: In this initial phase, the integration process is initiated through a Directed Acyclic Graph (DAG) script. The script serves as an orchestrator, delineating tasks and dependencies for a streamlined workflow. After connecting to MongoDB and extracting relevant data from specified collections, the script seamlessly uploads the dataset to Google Cloud Storage (GCS). This step ensures the data's accessibility for subsequent PySpark processing tasks, fostering a cohesive data flow across the cloud ecosystem.


2. **Run PySpark Job on Dataproc Cluster and Save Visualization to GCS**: Following the data upload to GCS, the workflow progresses to leverage PySpark and Dataproc for advanced data processing and visualization. Initially, a Dataproc cluster is dynamically created using the `google-cloud-dataproc` library, configuring parameters like instance count and types. Subsequently, a PySpark job is submitted to the cluster, executing data processing tasks. The resulting visualizations are saved back to GCS, ensuring the persistence of valuable insights. Finally, the Dataproc cluster is automatically deleted, optimizing resource usage and enhancing cost efficiency in the cloud environment. This comprehensive workflow seamlessly integrates MongoDB, PySpark, and Google Cloud Storage for efficient data handling and visualization in a distributed computing setting.

Use PySpark's powerful data processing capabilities combined with MongoDB's storage to craft dynamic dashboards and visualizations.

<p align="center">
  <kbd><img src="images/viz.png" width=600></img></kbd>
</p>
<p align="center"><em>Figure 6: Analyzing the Evolution of Authors' Soundscapes: Unveiling Insights from Track Histories and Audio Features</a></em></p>









