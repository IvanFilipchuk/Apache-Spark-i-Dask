from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace, length
from pyspark.sql.functions import desc
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark_wordcount_no_rdd <input_file_path> <word_length> <min_word_length>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    word_length = int(sys.argv[2])
    min_word_length = int(sys.argv[3])
    spark = SparkSession.builder \
        .appName("WordCountNoRDD") \
        .getOrCreate()
    lines = spark.read.text(input_file_path)
    words = lines.select(explode(split(lines.value, "\s+")).alias("word")) \
                 .withColumn("word", lower(regexp_replace("word", "[^a-zA-Z0-9]", "")))

    words_filtered = words.filter(length("word") == word_length)
    word_counts = words_filtered.groupBy("word").count()
    sorted_word_counts = word_counts.orderBy(desc("count"))
    print(f"Words with length {word_length}, sorted by occurrence:")
    sorted_word_counts.show()
    words_min_length = words.filter(length("word") >= min_word_length)
    word_counts_min_length = words_min_length.groupBy("word").count()
    sorted_word_counts_min_length = word_counts_min_length.orderBy(desc("count"))
    print(f"\nWords with minimum length {min_word_length}, sorted by occurrence:")
    sorted_word_counts_min_length.show()
    spark.stop()