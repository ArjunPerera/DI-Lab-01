import pytest
import requests
import json
import collect_got_info

hostname = 'localhost'
database = 'got'
username = 'got'
password = 'gotadmin'
port_id = 5555


# Test GOT api

def test_api():
    url = "https://thronesapi.com/api/v2/Characters"
    response = requests.get(url)
    assert response.status_code == 200
