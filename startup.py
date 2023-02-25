import etl.extract as extract
from etl.transform import Transform


def run(data):
    """
    Extract, transform and load Data
    """
    if data:
        transform = Transform()
        routine_customers, nroutine_customers = transform.run(data)
        # print(routine_customers)
        # print(nroutine_customers)
    #     load.run(data=parsed_data)
    # else:
    #     log.error(f"Extract data failed")


if __name__ == '__main__':
    data = extract.run()
    if data:
        run(data=data)
