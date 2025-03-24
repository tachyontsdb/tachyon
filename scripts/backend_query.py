import json

import requests

DEFAULT_BACKEND_URL = "http://localhost:8080"
DEFAULT_DB_PATH = "./tmp"


def main():
    backend_url_str = input(f"Please enter the backend url (or empty for the default \"{DEFAULT_BACKEND_URL}\"):")

    if backend_url_str == "":
        backend_url = DEFAULT_BACKEND_URL
    else:
        backend_url = backend_url_str

    health_check_response = requests.get(url=f"{backend_url}/health")
    if health_check_response.status_code != requests.codes.OK:
        print("The backend isn't running!")
        return

    path_str = input(f"Please enter the database path (or empty for the default \"{DEFAULT_DB_PATH}\"):")

    if path_str == "":
        path = DEFAULT_DB_PATH
    else:
        path = path_str

    get_streams_body = {"path": path}
    get_streams_response = requests.post(url=f"{backend_url}/get_streams", json=get_streams_body)

    print(f"Get streams response status code: {get_streams_response.status_code}")
    if get_streams_response.status_code == requests.codes.OK:
        print(json.dumps(get_streams_response.json(), indent=4))
    else:
        print(get_streams_response.content)

    query = input("Please enter a query:")

    start_str = input("Please enter a start time (or empty for None):")

    if start_str == "":
        start = None
    else:
        start = int(start)

    end_str = input("Please enter an end time (or empty for None):")

    if end_str == "":
        end = None
    else:
        end = int(end)

    query_body = {"path": path, "inner": {"query": query, "start": start, "end": end}}
    query_response = requests.post(url=f"{backend_url}/query", json=query_body)

    print(f"Query response status code: {query_response.status_code}")
    if query_response.status_code == requests.codes.OK:
        print(json.dumps(query_response.json(), indent=4))
    else:
        print(query_response.content)


if __name__ == "__main__":
    main()
