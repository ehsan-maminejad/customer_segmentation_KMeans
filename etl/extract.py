import requests

from_date = "1401/01/01"
to_date = "1402/09/06"
customer_code = '1418'
dataType = 2

# sales
if dataType == 2:
    customer_type = 2756
# after_sales
elif dataType == 3:
    customer_type = 2757


def run():
    url = f"https://api.havayar.com/crm/getCustomerSalesInfo?fromDate={from_date}&toDate={to_date}&customerCode={customer_code}&dataType={dataType}"

    payload = {}
    headers = {}

    response = requests.request(
        "GET", url, headers=headers, data=payload, timeout=300)

    if response.status_code == 200:
        return response.json()['Result']
