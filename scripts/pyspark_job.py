import sys
from subprocess import call
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas
from google.cloud import storage
from io import BytesIO

# The following is intended to be before the import Plotly statements.
# Install Python packages
call(["pip", "install", "kaleido==0.2.1", "plotly==5.19.0", "pandas", "numpy"])
# image_url = 'gs://msds697_final_project/piano.png'

import plotly.io as pio
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Set the bucket name
gcs_bucket_dag = 'bucket_for_dag'

# Initialize Spark session
spark = SparkSession.builder.appName("PlotlyOnDataproc").getOrCreate()

df = spark.read.json(f"gs://{gcs_bucket_dag}/data/Tracks_collection.json")

def min_max_normalize_tuple(tpl):
    normalized_tpl = []

    # Filter numeric values
    numeric_values = [val for val in tpl if isinstance(val, (int, float))]

    if numeric_values:
        # Calculate min and max only for numeric values
        min_val = min(numeric_values)
        max_val = max(numeric_values)

        # Avoid division by zero
        if max_val - min_val != 0:
            normalized_tpl = [(val - min_val) / (max_val - min_val)
                              if isinstance(val, (int, float)) else val for val in tpl]
        else:
            # Handle the case when all numeric values in the tuple are the same
            normalized_tpl = [0.0 if isinstance(
                val, (int, float)) else val for val in tpl]
    else:
        # If there are no numeric values in the tuple, keep it unchanged
        normalized_tpl = tpl

    return tuple(normalized_tpl)

# Aggregate the data
new_df = df.groupBy("user_name"). \
    agg(F.mean("loudness").alias("loudness"),
        F.mean("energy").alias("energy"),
        F.mean("speechiness").alias("speechiness"),
        F.mean("acousticness").alias("acousticness"),
        F.mean("instrumentalness").alias("instrumentalness"),
        F.mean("liveness").alias("liveness"),
        F.mean("valence").alias("valence"))
new_df = new_df.withColumn("index", F.monotonically_increasing_id())
new_df = new_df.select("index", "user_name", "speechiness", "loudness",
                       "acousticness", "instrumentalness", "liveness", "valence", "energy")
rows = new_df.collect()
data = [tuple(row) for row in rows]

# Normalize the data
normalized_data = [min_max_normalize_tuple(tpl) for tpl in data]

columns = ["Observation", "Name", "Speechiness", "Loudness",
           "Acousticness", "Instrumentalness", "Liveness", "Valence", "Energy"]

# Create a PySpark DataFrame
df = spark.createDataFrame(normalized_data, columns)

# Convert PySpark DataFrame to Pandas DataFrame for visualization
pandas_df = df.toPandas()

# Create radar chart using Plotly
fig = make_subplots(rows=2, cols=3,
                    specs=[[{'type': 'polar'}, {'type': 'polar'}, {'type': 'polar'}],
                           [{'type': 'polar'}, {'type': 'polar'}, {'type': 'polar'}]])

fig.add_trace(go.Scatterpolar(
    r=pandas_df.iloc[0, 2:],
    theta=df.columns[2:],
    fill='toself'
), row=1, col=1)

fig.add_trace(go.Scatterpolar(
    r=pandas_df.iloc[1, 2:],
    theta=df.columns[2:],
    fill='toself'
), row=1, col=2)

fig.add_trace(go.Scatterpolar(
    r=pandas_df.iloc[2, 2:],
    theta=df.columns[2:],
    fill='toself'
), row=1, col=3)

fig.add_trace(go.Scatterpolar(
    r=pandas_df.iloc[3, 2:],
    theta=df.columns[2:],
    fill='toself'
), row=2, col=1)

fig.add_trace(go.Scatterpolar(
    r=pandas_df.iloc[4, 2:],
    theta=df.columns[2:],
    fill='toself'
), row=2, col=2)


# Update layout for better visualization
fig.update_layout(
    polar=dict(
        radialaxis=dict(
            visible=True,
            range=[-10, 1]  # Adjust the range based on your data
        )),
    showlegend=False,
    width=1900,
    height=1000,
    margin=dict(t=150),
    paper_bgcolor='lightgray'
)

fig.update_polars(dict(radialaxis=dict(range=[0, 1])), row=1, col=1)
fig.update_polars(dict(radialaxis=dict(range=[0, 1])), row=1, col=2)
fig.update_polars(dict(radialaxis=dict(range=[0, 1])), row=1, col=3)
fig.update_polars(dict(radialaxis=dict(range=[0, 1])), row=2, col=1)
fig.update_polars(dict(radialaxis=dict(range=[0, 1])), row=2, col=2)


# Update annotations
feature_description = ['<b>Speechiness:</b> The presence of spoken words in a track',
                       '<b>Loudness:</b> The overall loudness of a track in decibels (dB)',
                       '<b>Acousticness:</b> A confidence measure of track acoustic',
                       '<b>Instrumentalness:</b> Predicts whether a track contains no vocals',
                       '<b>Liveness:</b> Detects the presence of an audience in the recording',
                       '<b>Valence:</b> Describes the musical positiveness conveyed by a track',
                       '<b>Energy:</b> Represents a perceptual measure of intensity and activity']


annotations = [dict(text='<b>Bhumika</b>', x=0.125, y=1.07, showarrow=False, xref='paper', yref='paper', font=dict(size=20)),
               dict(text='<b>Ireri</b>', x=0.5, y=1.07, showarrow=False,
                    xref='paper', yref='paper', font=dict(size=20)),
               dict(text='<b>Jessica</b>', x=0.85, y=1.07, showarrow=False,
                    xref='paper', yref='paper', font=dict(size=20)),
               dict(text='<b>Eren</b>', x=0.135, y=0.48, showarrow=False,
                    xref='paper', yref='paper', font=dict(size=20)),
               dict(text='<b>Yihan</b>', x=0.5, y=0.48, showarrow=False,
                    xref='paper', yref='paper', font=dict(size=20))
               ]

annotations.append(
    dict(text=feature_description[0],
         x=1.0, y=0.15,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[1],
         x=1.02, y=0.13,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[2],
         x=1.0, y=0.11,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[3],
         x=1.03, y=0.09,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[4],
         x=1.02, y=0.07,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[5],
         x=1.03, y=0.05,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text=feature_description[6],
         x=1.03, y=0.03,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=17, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text="<b>AUDIO FEATURES</b>",
         x=0.9, y=0.18,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=20, color='black', family='Courier New')
         )
)

annotations.append(
    dict(text="<b>Spotify Insights</b>",
         x=0.93, y=0.3,
         showarrow=False,
         xref='paper', yref='paper',
         font=dict(size=30, color='black', family='Courier New')
         )
)


fig.update_layout(annotations=annotations)

# fig.add_layout_image(
#     dict(
#         source=image_url,
#         xref="paper", yref="paper",
#         x=0.77, y=0.2,
#         sizex=0.05, sizey=0.05,
#         xanchor="center", yanchor="middle",
#     )
# )

# fig.add_layout_image(
#     dict(
#         source=image_url,
#         xref="paper", yref="paper",
#         x=0.93, y=0.2,
#         sizex=0.05, sizey=0.05,
#         xanchor="center", yanchor="middle",
#     )
# )

# Show the figure
# fig.show()

# Show or save the figure as needed
img_bytes = BytesIO()
pio.write_image(fig, img_bytes, format='png')
img_bytes.seek(0)


client = storage.Client()
bucket = client.bucket(gcs_bucket_dag)
blob = bucket.blob('images/radar_chart.png')
blob.upload_from_file(img_bytes, content_type='image/png')
print(f'Image has been saved to {gcs_bucket_dag}/images/radar_chart.png')
# Stop the Spark session
spark.stop()
