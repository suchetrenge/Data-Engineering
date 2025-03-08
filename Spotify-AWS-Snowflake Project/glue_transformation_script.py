
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode,col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_path = "s3://aws-de-s3-bucket-1807/raw/to_process/"

dynamicFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json",
)
df = dynamicFrame.toDF()
df.show()
#### Transformation

df = df.select("tracks")
df_data = df.withColumn("tracks_items",col("tracks.items")).select("tracks_items")
df_items = df_data.withColumn("items",explode("tracks_items")).select("items")

# Album Transformation
def album_transformation(df_items):
        df_album = df_items.withColumn("album_id",col("items.track.album.id"))\
            .withColumn("album_name",col("items.track.album.name"))\
            .withColumn("album_release_date",col("items.track.album.release_date"))\
            .withColumn("album_total_tracks",col("items.track.album.total_tracks"))\
            .withColumn("album_url",col("items.track.album.external_urls.spotify"))\
            .drop_duplicates(["album_id"])\
            .select("album_id","album_name","album_release_date","album_total_tracks","album_url")
        
        return df_album
# Songs Transformation
def songs_transformation(df_items):
    df_songs = df_items.withColumn("song_id",col("items.track.id"))\
                .withColumn("song_name",col("items.track.name"))\
                .withColumn("song_duration",col("items.track.duration_ms"))\
                .withColumn("song_url",col("items.track.external_urls.spotify"))\
                .withColumn("song_popularity",col("items.track.popularity"))\
                .withColumn("song_added",col("items.added_at"))\
                .withColumn("album_id",col("items.track.album.id"))\
                .withColumn("artist_id",col("items.track.album.artists").getItem(0).getItem("id"))\
                .drop_duplicates(["song_id"])\
                .select("song_id","song_name","song_duration","song_url","song_popularity","song_added","album_id","artist_id")
    
    return df_songs
# Songs Transformation
def artist_transformation(df_items):
    df_artists_temp = df_items.withColumn("artist_data",explode(col("items.track.artists")))
    df_artists = df_artists_temp.withColumn("artist_id",col("artist_data.id"))\
                            .withColumn("artist_name",col("artist_data.name")) \
                            .withColumn("external_url",col("artist_data.href"))\
                            .drop_duplicates(["artist_id"])\
                            .select("artist_id", "artist_name", "external_url")
    return df_artists
df_album = album_transformation(df_items)
df_songs = songs_transformation(df_items)
df_artists = artist_transformation(df_items)
def write_to_s3(df, path_suffix, format_type="csv"):
    # Convert back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://aws-de-s3-bucket-1807/transformed/{path_suffix}/"},
        format = format_type
    )
#write data to s3   
write_to_s3(df_album, "album/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(df_songs, "songs/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(df_artists, "artist/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
datetime.now().strftime("%Y-%m-%d")
job.commit()