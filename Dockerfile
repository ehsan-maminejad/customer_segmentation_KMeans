# 
FROM 172.20.30.73:5000/ai/python:customized as python

#

COPY ./requirements.txt ./requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

from python as runner

WORKDIR /app
# 
COPY ./ /app/

#EXPOSE 80
#
CMD ["python","api/main.py"]