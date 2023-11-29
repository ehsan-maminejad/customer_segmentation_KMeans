import os
import sys

import uvicorn
from fastapi import FastAPI, HTTPException
from typing import List
from pydantic import BaseModel
from starlette.responses import FileResponse

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_path)

from startup import run
import etl.extract as extract
from etl.transform import Transform

app = FastAPI()


class DataList(BaseModel):
    data: List[dict]


@app.post("/customer_data/", status_code=200)
async def customer_data(data_list: DataList, customer_type: int):
    # Process the list of dictionaries
    customer_data = data_list
    # 2756 is sale
    if any(customer_data.data) and customer_type == 2756:
        try:
            run(customer_data.data)
            return {"message": "Sale unit data received and processed successfully"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"error:{str(e)}, message: process was unsuccessful")
    # 2757 is after sale
    elif any(customer_data.data) and customer_type == 2757:
        try:
            # run(customer_data.data)
            return {"message": "After sale service unit data received and processed successfully"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"error:{str(e)}, message: process was unsuccessful")
    else:
        raise HTTPException(status_code=400, detail="No data provided")


@app.get("/")
async def read_index():
    return FileResponse(f'{root_path}/api/index.html')


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10001)
