FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY app ./app
COPY web ./web
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh && mkdir -p /data

EXPOSE 8000 8001 8002
ENTRYPOINT ["./entrypoint.sh"]
CMD ["webapp"]
