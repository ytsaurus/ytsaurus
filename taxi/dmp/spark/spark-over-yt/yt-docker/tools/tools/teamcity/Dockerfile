ARG BASE_IMAGE_TAG
FROM spyt-test-base:${BASE_IMAGE_TAG:-latest}

# This image copies whole application from its context to avoid
# exporting project from arc and mounting it as a volume
COPY . /app
