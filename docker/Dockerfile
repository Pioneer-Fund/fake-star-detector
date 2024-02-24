FROM python:3.10-slim
EXPOSE 3000
COPY ./ fake-star-detector/
WORKDIR /fake-star-detector
RUN apt-get update && \
    apt-get install -y gcc python3-dev
RUN pip install -e ".[dev]"
CMD ["dagster", "dev", "-h", "0.0.0.0"]