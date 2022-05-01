import requests
from prefect import flow, task

@task
def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    print(response.json())
    return response.json()

@task
def parse_fact(response):
    print(response[0]["display_name"])
    return

@task
def make_output():
    return 'hiii'

@task
def use_output(inp):
    a = make_output()
    print(a[0])
    return a[1]

@flow
def api_flow(url):
    fact_json = call_api(url)
    parse_fact(fact_json)
    print('hup', use_output)
    return

if __name__ == '__main__':
    url = "https://nominatim.openstreetmap.org/search?city=houston&format=json"
    api_flow(url)