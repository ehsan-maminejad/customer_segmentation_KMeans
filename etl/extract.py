import requests

from_date = "1397/01/01"
to_date = "1401/12/01"
customer_code = ''


def run():
    url = f"https://api.havayar.com/crm/getCustomerSalesInfo?fromDate={from_date}&toDate={to_date}&customerCode={customer_code}"

    payload = {}
    headers = {}

    response = requests.request(
        "GET", url, headers=headers, data=payload, timeout=300)

    if response.status_code == 200:
        return response.json()['result']
