import etl.extract as extract
from etl.transform import Transform
from etl.load import Load


def run(data,customer_type,current_date=None):
    """
    Extract, transform and load Data
    """
    if data:
        transform = Transform(current_date)
        load = Load()
        if customer_type == 2756:
            routine_customers, nroutine_customers = transform.run(data, customer_type)
            response = load.run(customer_type, routine_customers, nroutine_customers)
            print(response.text)
        elif customer_type == 2757:
            after_sales_customers = transform.run(data, customer_type)
            response = load.run(customer_type, None, None, after_sales_customers)
            print(response.text)


if __name__ == '__main__':
    data = extract.run()
    if data:
        run(data, extract.customer_type)
