import etl.extract as extract
from etl.transform import Transform
from etl.load import Load


def run(data):
    """
    Extract, transform and load Data
    """
    if data:
        transform = Transform()
        load = Load()
        routine_customers, nroutine_customers = transform.run(data)
        response = load.run(routine_customers, nroutine_customers)
        print(response.text)


if __name__ == '__main__':
    data = extract.run()
    if data:
        run(data=data)
