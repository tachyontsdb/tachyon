import asyncio
import json
import urllib.parse

import websockets


async def perform_query():
    base_uri = "ws://localhost:8080/ws/query"

    params = {
        "path": "./tmp",
        "queries": "profit{country=\"canada\",division=\"software\"}",
        "interval_milliseconds": 50,
        # Optional fields can be added if needed, e.g.:
        "time_begin": "0",
        "time_end": "1400",
        "time_diff": "300",
    }

    # Remove keys with None values.
    params = {k: v for k, v in params.items() if v is not None}

    # Encode parameters into a query string.
    query_string = urllib.parse.urlencode(params)
    uri = f"{base_uri}?{query_string}"

    async with websockets.connect(uri) as websocket:
        print("Connected to the backend with query parameters.")

        try:
            while True:
                response = await websocket.recv()
                # Try to parse and pretty print the JSON response.
                try:
                    response_obj = json.loads(response)
                    print("Received response:", json.dumps(response_obj, indent=2))
                except json.JSONDecodeError:
                    print("Received non-JSON response:", response)
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed by the server.")


if __name__ == "__main__":
    asyncio.run(perform_query())
