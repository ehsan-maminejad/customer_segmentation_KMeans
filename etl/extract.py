import requests

from_date = "1381/01/01"
to_date = "1400/05/31"
customer_code = '81'


def run():
    url = f"https://api.havayar.com/crm/getCustomerSalesInfo?fromDate={from_date}&toDate={to_date}&customerCode={customer_code}"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()['result']
