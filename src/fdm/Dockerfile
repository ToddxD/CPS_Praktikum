FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "run.py" ]
