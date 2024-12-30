from itertools import combinations
from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext()
print("Spark Context initialized")
# Load the order_products_prior.csv and order_products_train.csv
data_prior = sc.textFile("s3://instacartbucket/order_products__prior.csv")
data_train = sc.textFile("s3://instacartbucket/order_products__train.csv")

# Skip header and combine both datasets
header = data_prior.first()
data_prior = data_prior.filter(lambda row: row != header)
data_train = data_train.filter(lambda row: row != header)
combined_data = data_prior.union(data_train)

print("*****Data loaded******")


# Mapper: Extract (order_id, product_id) pairs
def parse_line(line):
    
    fields = line.split(',')
    return (fields[0], fields[1])

order_products = combined_data.map(parse_line)

# Group by order_id and get all pairs of products for each order
def generate_pairs(order):
    products = order[1]
    return combinations(sorted(products), 2)

print("*****Pairs Generated******")

order_groups = order_products.groupByKey().flatMap(generate_pairs)

# Count the frequency of each pair
pair_counts = order_groups.map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a + b)

# Save the result as text file for reducer
pair_counts.saveAsTextFile("s3://instacartbucket/output/pair_counts")
print("*****Pairs Counted******")

# Reducer: Aggregating counts and mapping product ids to names
# Load products.csv
products = sc.textFile("s3://instacartbucket/products.csv")
product_map = products.map(lambda line: line.split(',')).map(lambda x: (x[0], x[1])).collectAsMap()

# Add product names to the pairs
def map_product_names(pair_count):
    (pair, count) = pair_count
    product1, product2 = pair
    product1_name = product_map.get(product1, "Unknown")
    product2_name = product_map.get(product2, "Unknown")
    return ((product1_name, product2_name), count)

named_pairs = pair_counts.map(map_product_names)

print("*****Product Names Mapped******")

# Sort by count in descending order
sorted_pairs = named_pairs.sortBy(lambda x: -x[1])

# Collect the top 10 pairs
top_ten_pairs = sorted_pairs.take(10)

# Count the total number of pairs
total_pairs = sorted_pairs.count()

# Prepare the output to be written into a file
output_messages = []
output_messages.append("Top Ten Pairs of Products Bought Together:")
for pair in top_ten_pairs:
    output_messages.append(str(pair))

output_messages.append(f"\nTotal Number of Pairs Created: {total_pairs}")

# Convert the list of messages to an RDD
output_rdd = sc.parallelize(output_messages)

# Save the results to the specified S3 path
output_rdd.saveAsTextFile("s3://instacartbucket/output/output_pair_and_count.txt")


# Print the results
print("Top Ten Pairs of Products Bought Tog ether:")
for pair in top_ten_pairs:
    print(pair)

print(f"\nTotal Number of Pairs Created: {total_pairs}")

# Save the result
sorted_pairs.saveAsTextFile("s3://instacartbucket/output/sorted_product_pairs")