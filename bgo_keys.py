import csv

# Step 1: collect bgg_ids from file2
bgg_ids = set()
with open("bgo_key_bgg_map.tsv", newline='', encoding="utf-8") as f2:
    reader = csv.DictReader(f2, delimiter='\t')
    for row in reader:
        try:
            bgg_ids.add(int(row["bgg_id"]))
        except (ValueError, KeyError):
            continue  # skip bad rows

print(f"Loaded {len(bgg_ids)} BGG IDs")

# Step 2: filter file1
kept = 0
with open("boardgames_ranks.csv", newline='', encoding="utf-8") as f1, \
     open("filtered_boardgames_ranks.csv", "w", newline='', encoding="utf-8") as out:

    reader = csv.DictReader(f1)
    writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
    writer.writeheader()

    for row in reader:
        try:
            if int(row["id"]) in bgg_ids:
                writer.writerow(row)
                kept += 1
        except (ValueError, KeyError):
            continue

print(f"Kept {kept} matching rows")