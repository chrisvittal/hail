FROM python:3.7-alpine
MAINTAINER Hail Team <hail@broadinstitute.org>

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scorecard /scorecard

EXPOSE 5000

CMD ["python", "/scorecard/scorecard.py"]
