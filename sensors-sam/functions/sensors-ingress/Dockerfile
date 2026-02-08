FROM public.ecr.aws/lambda/python:3.12

COPY ./src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t .
COPY ./src .

# Command can be overwritten by providing a different command in the template directly.
CMD ["sensors_ingress.lambda_handler"]