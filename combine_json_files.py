import os
import json
from pathlib import Path
import re

def extract_timestamp_from_filename(filename):
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    return match.group(0) if match else None

def combine_json_files(repo_name, data_type, output_file):
    traffic_stats_dir = Path("traffic-stats")
    combined_data = {}

    # Iterate over all JSON files for the specific data type in the directory
    for json_file in traffic_stats_dir.glob(f"{repo_name}-{data_type}-*.json"):
        timestamp = extract_timestamp_from_filename(json_file.name)
        with open(json_file, 'r') as file:
            data = json.load(file)
            for entry in data:
                if data_type in ["views", "clones"]:
                    for item in entry.get("data", {}).get(data_type, []):
                        ts = item["timestamp"]
                        if ts not in combined_data:
                            combined_data[ts] = {"count": 0, "uniques": 0}
                        # Take the max count and uniques values for each timestamp
                        combined_data[ts]["count"] = max(item["count"], combined_data[ts]["count"])
                        combined_data[ts]["uniques"] = max(item["uniques"], combined_data[ts]["uniques"])
                else:
                    for item in entry.get("data", []):
                        ts = timestamp
                        if ts not in combined_data:
                            combined_data[ts] = {}
                        
                        if data_type == "referrers":
                            referrer = item["referrer"]
                            if referrer not in combined_data[ts]:
                                combined_data[ts][referrer] = {"count": 0, "uniques": 0}
                            # Take the max count and uniques values for each referrer
                            combined_data[ts][referrer]["count"] = max(item["count"], combined_data[ts][referrer]["count"])
                            combined_data[ts][referrer]["uniques"] = max(item["uniques"], combined_data[ts][referrer]["uniques"])

                        elif data_type == "paths":
                            path = item["path"]
                            if path not in combined_data[ts]:
                                combined_data[ts][path] = {"title": item["title"], "count": 0, "uniques": 0}
                            # Take the max count and uniques values for each path
                            combined_data[ts][path]["count"] = max(item["count"], combined_data[ts][path]["count"])
                            combined_data[ts][path]["uniques"] = max(item["uniques"], combined_data[ts][path]["uniques"])

    # Convert combined_data to a list of dictionaries for JSON serialization
    if data_type in ["views", "clones"]:
        combined_list = [{"timestamp": ts, "count": data["count"], "uniques": data["uniques"]} for ts, data in combined_data.items()]
    elif data_type == "referrers":
        combined_list = [{"timestamp": ts, "data": [{"referrer": referrer, "count": referrer_data["count"], "uniques": referrer_data["uniques"]} for referrer, referrer_data in data.items()]} for ts, data in combined_data.items()]
    elif data_type == "paths":
        combined_list = [{"timestamp": ts, "data": [{"path": path, "title": path_data["title"], "count": path_data["count"], "uniques": path_data["uniques"]} for path, path_data in data.items()]} for ts, data in combined_data.items()]
        
    # Sort combined_list by timestamp
    combined_list = sorted(combined_list, key=lambda x: x["timestamp"])

    # Write combined data to the output file
    with open(output_file, 'w') as file:
        json.dump(combined_list, file, indent=4)

def main():
    repos = [
        "castepconv",
        "ccpnc-database",
        "ccpnc-nomad-oasis",
        "crystcif-parse",
        "crystvis-js",
        "dipolar_averages",
        "magresview-2",
        "magresview",
        "make-supercell",
        "metrics",
        "nomad-parser-magres",
        "orgdisord",
        "parse-fmt",
        "pspot-site",
        "pynics",
        "soprano",
        "staged-recipes"
    ]  # List of repository names
    data_types = ["views", "paths", "referrers", "clones"]  # List of data types

    for repo in repos:
        for data_type in data_types:
            output_file = f"traffic-stats/{repo}-{data_type}-combined.json"
            combine_json_files(repo, data_type, output_file)

if __name__ == "__main__":
    main()