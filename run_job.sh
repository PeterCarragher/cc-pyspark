#!/bin/bash

# Iterate over sub-CSV files
for sub_csv_file in "/mnt/d/cc/splits_v2/"sub_csv_*; do
    # Define output iteration ID based on sub-CSV file name
    output_iteration_id=$(basename "$sub_csv_file" | cut -d'_' -f3)
    output_iteration_id=$(echo $output_iteration_id | cut -d'.' -f1)
    # Run spark-submit command for each sub-CSV
    spark-submit --executor-cores 18 --executor-memory 4g --driver-memory 170g ./wat_extract_links.py \
        --log_level WARN --input_base_url "https://data.commoncrawl.org/" --input_table_format csv --input_table_option header=True --num_input_partitions 18 $sub_csv_file "output_table_$output_iteration_id"

    wait
done
