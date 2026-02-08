FROM public.ecr.aws/lambda/python:3.12

# Function dependencies
COPY functions/sensors-ingress/src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t .

# Shared helpers (previously provided as a Lambda Layer)
COPY layers/helpers/python/helpers /opt/python/helpers

# Function source
COPY functions/sensors-ingress/src .

# Command can be overwritten by providing a different command in the template directly.
CMD ["sensors_ingress.lambda_handler"]