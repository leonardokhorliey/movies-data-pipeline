import pyspark
from pyspark.sql import SparkSession, functions as F
import argparse


def run_spark_job(bucket, bq_dataset, path):

    spark = SparkSession.builder \
        .appName('de-zoomcamp-project') \
        .getOrCreate()

    spark.conf.set('temporaryGcsBucket', bucket)


    genome_scores_df = spark.read \
        .option("header", "true") \
        .parquet(f'{path}/genome-scores.parquet')

    genome_tags_df = spark.read \
        .option("header", "true") \
        .parquet(f'{path}/genome-tags.parquet')


    ratings_df = spark.read \
        .option("header", "true") \
        .parquet(f'{path}/ratings/')

    movies_df = spark.read \
        .option("header", "true") \
        .parquet(f'{path}/movies.parquet')


    genres_df = spark.read \
        .option("header", "true") \
        .parquet(f'{path}/genres.parquet')



    genomes = genome_scores_df.groupBy('tagId').agg(F.avg('relevance').alias('avg_relevance'))\
            .join(genome_tags_df, ['tagId'], how="left")


    ratings_with_movies = ratings_df.join(movies_df, ['movieId'], how="left")\
            .withColumnRenamed('title', 'movie_title')



    genres_df.write.format('bigquery') \
    .option('table', f'{bq_dataset}.genres') \
    .save()


    genomes.write.format('bigquery') \
    .option('table', f'{bq_dataset}.genomes') \
    .save()

    ratings_with_movies.write.format('bigquery') \
    .option('table', f'{bq_dataset}.movie_ratings') \
    .save()


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-p", "--path", type= str, help="Path to file in buckets")
    argParser.add_argument("-d", "--dataset", type= str, help="Bigquery dataset to be used")
    argParser.add_argument("-b", "--bucket", type= str, help="GCS Bucket to use")

    args = argParser.parse_args()

    run_spark_job(args.bucket, args.dataset, args.path)

