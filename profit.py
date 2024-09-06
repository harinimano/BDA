from pyspark import SparkConf, SparkContext

def main():
    # Initialize a SparkConf and SparkContext
    conf = SparkConf().setAppName("TotalProfitByCountry")
    sc = SparkContext(conf=conf)

    # Load the data from the CSV file
    lines = sc.textFile("Global_Superstore2.csv")

    # Skip the header row
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    # Map step: Emit (Country, Profit) for each row
    country_profit = lines.map(lambda line: line.split(',')) \
                          .map(lambda fields: (fields[10], float(fields[20])))

    # Reduce step: Aggregate profits by country
    total_profit_by_country = country_profit.reduceByKey(lambda a, b: a + b)

    # Save the results to a text file
    total_profit_by_country.saveAsTextFile("total_profit_by_country_output")

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
