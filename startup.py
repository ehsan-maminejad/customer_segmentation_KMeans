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
        # routine_rank, nroutine_rank = load.predict_class(routine_customers, nroutine_customers)
        # print(routine_customers)
        response = load.run(routine_customers, nroutine_customers)
        print(response.text)
    # else:
    #     log.error(f"Extract data failed")


if __name__ == '__main__':
    data = extract.run()
    if data:
        run(data=data)
