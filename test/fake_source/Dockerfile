# Wraps the fake Airbyte-protocol source in an image for the opt-in Docker-mode test.
FROM python:3.13-alpine
COPY source-fake /source-fake
ENTRYPOINT ["python3", "/source-fake"]
