import sys
import os
import csv

#system arguments
input_csv = sys.argv[1]
output_csv = sys.argv[2]

#generate sum of revenue using a streaming generator
def revenue_generator(filename):
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            product_id = row['Product ID']
            cust_id = row['Customer ID']
            product_rev = row['Item Cost']
            yield(product_id, product_rev)
			

prod_revenue = {}
for product_id, product_rev in revenue_generator(input_csv):
    key = product_id
    if key not in prod_revenue:
        prod_revenue[key] = []
    prod_revenue[key].append(product_rev)
    agg_rev = {}
    for key in prod_revenue:
        agg_rev[key] = list(map(float,prod_revenue[key]))
    agg_rev = {k:round(sum(v),2) for k,v in agg_rev.items()}
	

cust_counts = {}
for product_id, cust_id in revenue_generator(input_csv):
    key = product_id
    if key not in cust_counts:
        cust_counts[key] = []
    cust_counts[key].append(cust_id)
    agg_cust = {}
    for key in cust_counts:
        agg_cust[key] = list(set(cust_counts[key]))
    agg_cust = {k:len(v) for k,v in agg_cust.items()}
	

csv_rows = zip(list(agg_cust.keys()),list(agg_cust.values()),list(agg_rev.values()))
csv_cols = ['Product ID','Customer Count','Total Revenue']

with open(output_csv, "w") as f:
    writer = csv.writer(f)
    writer.writerow(csv_cols)
    for row in csv_rows:
        writer.writerow(row)