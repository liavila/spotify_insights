from pyspark.sql import SparkSession
from subprocess import call

# Install Python packages
call(["pip", "install", "kaleido==0.2.1", "plotly==5.19.0", "pandas", "numpy"])

import pandas
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from google.cloud import storage
from io import BytesIO

# Initialize Spark session
spark = SparkSession.builder.appName("PlotlyOnDataproc").getOrCreate()

# Sample data for each feature for five observations
data = [
    (1, "Jessica", 0.8, -5, 0.2, 0.5, 0.3, 0.7, 120),
    (2, "Ireri", 0.5, -8, 0.2, 0.3, 0.6, 0.5, 100),
    (3, "Yihan", 0.2, -3, 0.6, 0.8, 0.4, 0.8, 140),
    (4, "Eren", 0.7, -6, 0.4, 0.2, 0.5, 0.6, 105),
    (5, "Bhumika", 0.4, -7, 0.1, 0.9, 0.2, 0.9, 115),
]

columns = ["Observation", "Name", "Speechiness", "Loudness", "Acousticness", "Instrumentalness", "Liveness", "Valence", "Tempo"]

# Create a PySpark DataFrame
df = spark.createDataFrame(data, columns)

# Convert PySpark DataFrame to Pandas DataFrame for visualization
pandas_df = df.toPandas()

# Create radar chart using Plotly
fig = make_subplots(rows=2, cols=3,
                    specs=[[{'type': 'polar'}, {'type': 'polar'}, {'type': 'polar'}],
                           [{'type': 'polar'}, {'type': 'polar'}, {'type': 'polar'}]])

for i in range(5):
    fig.add_trace(go.Scatterpolar(
        r=pandas_df.iloc[i, 2:],
        theta=df.columns[2:],
        fill='toself',
        name=pandas_df.iloc[i, 1]
    ), row=(i // 3) + 1, col=(i % 3) + 1)

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

# Update polar axis range for each subplot
for i in range(1, 6):
    fig.update_polars(dict(radialaxis=dict(range=[-10, 1])), row=(i // 3) + 1, col=(i % 3) + 1)

# Update annotations
feature_description = ['<b>Speechiness:</b> The presence of spoken words in a track',
                          '<b>Loudness:</b> The overall loudness of a track in decibels (dB)',
                            '<b>Acousticness:</b> A confidence measure of track acoustic',
                            '<b>Instrumentalness:</b> Predicts whether a track contains no vocals',
                            '<b>Liveness:</b> Detects the presence of an audience in the recording',
                            '<b>Valence:</b> Describes the musical positiveness conveyed by a track',
                            '<b>Tempo:</b> The overall estimated tempo of a track in BPM']


annotations=[dict(text='<b>Jessica</b>', x=0.125, y=1.07, showarrow=False, xref='paper', yref='paper', font=dict(size=20)),
            dict(text='<b>Ireri</b>', x=0.5, y=1.07, showarrow=False, xref='paper', yref='paper', font=dict(size=20)),
            dict(text='<b>Yihan</b>', x=0.85, y=1.07, showarrow=False, xref='paper', yref='paper', font=dict(size=20)),
            dict(text='<b>Eren</b>', x=0.135, y=0.48, showarrow=False, xref='paper', yref='paper', font=dict(size=20)),
            dict(text='<b>Bhumika</b>', x=0.5, y=0.48, showarrow=False, xref='paper', yref='paper', font=dict(size=20))
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
         x=1.0, y=0.03,
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


fig.update_layout(annotations=annotations)

image_url = 'piano.png'

fig.add_layout_image(
    dict(
        source=image_url,
        xref="paper", yref="paper",
        x=0.77, y=0.2,
        sizex=0.05, sizey=0.05,
        xanchor="center", yanchor="middle",
    )
)

fig.add_layout_image(
    dict(
        source=image_url,
        xref="paper", yref="paper",
        x=0.93, y=0.2,
        sizex=0.05, sizey=0.05,
        xanchor="center", yanchor="middle",
    )
)

# Show the figure
# fig.show()

# Show or save the figure as needed
img_bytes = BytesIO()
pio.write_image(fig, img_bytes, format='png')
img_bytes.seek(0)

client = storage.Client()
bucket = client.bucket('msds697_final_project')
blob = bucket.blob('radar_chart.png')
blob.upload_from_file(img_bytes, content_type='image/png')

# Stop the Spark session
spark.stop()
