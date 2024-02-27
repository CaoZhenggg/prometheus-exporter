FROM python:3.10.13-bullseye

WORKDIR /app/

RUN mkdir /root/.pip/
COPY pip.conf /root/.pip/

COPY requirements.txt /app/

RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

COPY main.py /app/

CMD ["gunicorn", "-b", "0.0.0.0:8000", "main:app", "-k", "uvicorn.workers.UvicornWorker"]
