import requests
import time

def get_leader_url():
    response = requests.get('http://node0:12121/leader')
    if response.status_code == 200:
        return response.json()['leader']
    else:
        raise Exception("No leader available")

def test_create_resource():
    leader_url = get_leader_url() + '/resource'
    response = requests.post(leader_url, json={'key': 'test', 'value': 123})
    assert response.status_code == 201
    assert response.json()['result'] == 'created'

def test_get_resource():
    leader_url = get_leader_url() + '/resource/test'
    response = requests.get(leader_url)
    assert response.status_code == 200
    assert response.json()['value'] == 123

def test_leader_redirect():
    leader_url = get_leader_url() + '/resource/test'
    
    response = requests.get(leader_url, allow_redirects=False)
    assert response.status_code == 302
    
    if 'Location' in response.headers:
        redirect_url = response.headers['Location']
        assert leader_url != redirect_url
    else:
        raise AssertionError("Redirect location header missing")

def test_update_resource():
    leader_url = get_leader_url() + '/resource/test'
    response = requests.put(leader_url, json={'value': 456})
    assert response.status_code == 200
    assert response.json()['result'] == 'updated'
    
    response = requests.get(leader_url)
    assert response.json()['value'] == 456

def test_delete_resource():
    leader_url = get_leader_url() + '/resource/test'
    response = requests.delete(leader_url)
    assert response.status_code == 200
    assert response.json()['result'] == 'deleted'

    response = requests.get(leader_url)
    assert response.status_code == 404

def test_cas_update_successful():
    leader_url = get_leader_url() + '/resource/test'

    response = requests.post(get_leader_url() + '/resource', json={'key': 'test', 'value': 123})
    assert response.status_code == 201
    assert response.json()['result'] == 'created'

    initial_get_response = requests.get(leader_url)
    initial_value = initial_get_response.json()['value']

    cas_response = requests.patch(leader_url, json={
        'expected_value': initial_value,
        'new_value': 789
    })
    assert cas_response.status_code == 200
    assert cas_response.json()['result'] == 'updated'

    final_get_response = requests.get(leader_url)
    assert final_get_response.json()['value'] == 789

def test_cas_update_unsuccessful():
    leader_url = get_leader_url() + '/resource/test'

    cas_response = requests.patch(leader_url, json={
        'expected_value': 999,
        'new_value': 321
    })
    assert cas_response.status_code == 409

    final_get_response = requests.get(leader_url)
    assert final_get_response.json()['value'] == 789