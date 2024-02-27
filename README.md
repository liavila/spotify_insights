# Distributed Data Processing with Apache Airflow, MongoDB, and SparkSQL

## Approach / Tentative Outline

**Proposal**: Develop a data pipeline in Airflow that can be used to build an application to work as a localized social media platform for small groups of friends. The application will allow users to connect their Spotify accounts and share their listening history. We perform analytics on the data to determine who listened to what the most, and who listened to the same songs the most, and other analytics based on the original datasets.

**Tech Stack:** Apache Airflow, MongoDB, SparkSQL, Google Cloud Platform, Spotify API, Python, PyMongo, Spotipy, SparkML, MongoDB Atlas, Flask, Jinja2.

Use Airflow to orchestrate the following tasks via CRON jobs or other scheduling methods:

1. Use the script to connect to the Spotify API and collect data user's in a friend group.
2. Load the data into Google Cloud Storage (GCS)
3. Import the data into MongoDB in a collection, we can use the `pymongo` library to interact with MongoDB.
4. Create new datasets from analytical data such user's favorite artists, songs, etc. based on the original datasets.
5. Store the aggregates in a separate collection in MongoDB (`pymongo`) on MongoDB Atlas (cloud-based MongoDB service).
6. Query the data using MongoDB / NoSQL queries.
7. Create Dataframes from the data from MongoDB Atlas.
8. (Optional) Use a recommendation system to recommend songs to users based on their group listening history, via SparkSQL / SparkML.
9. Run SparkSQL queries over the data frames.
10. Refresh a dashboard with the latest data from MongoDB Atlas, using Flask and Jinja2.


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

The access token is a string which contains the credentials and permissions that can be used to access a given resource (e.g artists, albums or tracks) or user's data (e.g your profile or your playlists). Your application requests authorization to access service resources from the user and the service then issues access tokens to the application. Note that the access token is valid for 1 hour (3600 seconds). After that time, the token expires and you need to request a new one. If we use the `spotipy` library, it will handle the token refresh for us. After 60 days the refresh token will expire and the user will need to re-authenticate. There is a `prompt_for_user_token` method in the `spotipy` library that can be used to get the access token. 

If we want to avoid having to re-authenticate every 60 days, we can use the [Authorization Code Flow with Proof Key for Code Exchange (PKCE)](https://developer.spotify.com/documentation/web-api/tutorials/code-pkce-flow) to get a refresh token. This method is suitable for mobile and desktop applications. It is recommended for applications that cannot store the client secret securely. Or we can use the [Authorization Code Flow](https://developer.spotify.com/documentation/web-api/tutorials/code-flow) to get a refresh token. This method is suitable for web applications. It is recommended for applications that can store the client secret securely.

## Step 2: Load the data into Google Cloud Storage (GCS)



