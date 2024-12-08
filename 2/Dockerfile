FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install flask requests pytest

COPY node.py .
COPY tests.py .

CMD ["python", "-u", "node.py"]