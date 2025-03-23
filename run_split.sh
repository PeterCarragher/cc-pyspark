split -l 10000 --numeric-suffixes=1 -a 4 /home/peter/dev/cc/data/cc-warc-index-2.csv /mnt/d/cc/splits_v2/sub_csv_
for sub_csv_file in /mnt/d/cc/splits_v2/sub_csv_*; do     
    cat "/mnt/d/cc/splits_v2/header.csv" "$sub_csv_file" > "$sub_csv_file".csv
    rm "$sub_csv_file"
done