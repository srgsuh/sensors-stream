FROM public.ecr.aws/lambda/python:3.12

# Function dependencies
COPY functions/sensors-low-values/src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t .

# Shared helpers
COPY layers/helpers/python/helpers /opt/python/helpers

# Function source
COPY functions/sensors-low-values/src .

CMD ["sensors_low_values.lambda_handler"]
