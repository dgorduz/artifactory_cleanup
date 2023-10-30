import requests
import json
import csv
import concurrent.futures

base_url = 'https://your_company.jfrog.io/artifactory/'
headers = {'Content-Type': 'text/plain',
            'Authorization': 'Bearer your_token'
            }

def write_to_file(file_name, data):
        with open(file_name, 'w') as json_file:
            json.dump(data, json_file, indent=4)

def export_to_json_grouped(output_file_name, data):
    grouped_data = {}

    for item in data:
        repo = item['repo']
        if repo not in grouped_data:
            grouped_data[repo] = []

        grouped_data[repo].append(item)

    write_to_file(output_file_name, grouped_data)

    num_repos = len(grouped_data)
    print(f"Total number of repositories: {num_repos}")

def export_to_csv(csv_file_name, candidates_for_deletion):
    with open(csv_file_name, 'w', newline='') as csvfile:
        fieldnames = ['last_downloaded', 'download_count', 'repo', 'path', 'name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(candidates_for_deletion)

def combine_jsons(json_one, json_two):
    json1_str = json.dumps(json_one)
    json2_str = json.dumps(json_two)

    # Add the two strings together
    combined_json_str = json1_str + json2_str

    # Convert the combined string back to a JSON object
    combined_json = json.loads(combined_json_str)
    return(combined_json)

def get_download_count(repo, path, name):
    download_url = f"{base_url}/api/storage/{repo}/{path}/{name}?stats"
    response_data = requests.get(download_url, headers=headers)
    if response_data.status_code == 200:
        response_data = response_data.json()
        return response_data['downloadCount']
    else:
        response_data = {"downloadCount": "N/A"}
        return response_data


def get_old_artifacts():

    data_query = """items.find({
                    "name": {
                        "$match": "*"
                    },
                    "type": "file",
                    "stat.downloaded": {
                        "$before": "1y"
                    }
                }).include("stat.downloaded")"""

    never_downloaded_query = """items.find({
                "name": {
                    "$match": "*"
                },
                "type": "file",
                "$and": [
                    {
                        "modified": {
                            "$before": "1y"
                        }
                    },
                    {
                        "stat.downloaded": {
                            "$eq": null
                        }
                    }
                ]
            }).include("stat.downloaded")"""

    response_data = requests.post(base_url+'api/search/aql',
                             headers=headers, data=data_query)
    never_downloaded_resp = requests.post(base_url+'api/search/aql', headers=headers, data=never_downloaded_query)

    # Parse JSON responses
    response_data = response_data.json()
    never_downloaded_data = never_downloaded_resp.json()

    # Merge the JSON responses into a single dictionary
    results = response_data["results"] + never_downloaded_data["results"]
    write_to_file('raw_combined.json', results)
    candidates_for_deletion = []
    results = {}

    with open("raw_combined.json", 'r') as json_file:
        results = json.load(json_file)


    # Split the results into batches of 200
    batch_size = 200
    # If failed due to too many requests start from the last request
    start_point = 6000
    for i in range(start_point, len(results), batch_size):
        batch = results[i:i + batch_size]

        # Create a ThreadPoolExecutor to send requests in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Use map to send multiple requests in parallel and collect the results
            download_counts = list(executor.map(
                lambda res: get_download_count(res['repo'], res['path'], res['name']), batch
            ))

        for res, download_count in zip(batch, download_counts):
            if 'stats' in res and res['stats']:
                last_downloaded = res['stats'][0]['downloaded']
            else:
                last_downloaded = 'never_downloaded'

            candidate = {
                'last_downloaded': last_downloaded,
                'download_count': download_count,
                'repo': res['repo'],
                'path': res['path'],
                'name': res['name']
            }

            candidates_for_deletion.append(candidate)
            export_to_csv("repositories_data.csv", candidates_for_deletion)

    export_to_json_grouped("repos_for_deletion.json", candidates_for_deletion)


if __name__ == '__main__':
    get_old_artifacts()
