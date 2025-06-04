import requests
import json

# Send test event to local container
response = requests.post(
    'http://localhost:9000/2015-03-31/functions/function/invocations',
    json={}
)
print(response.json())