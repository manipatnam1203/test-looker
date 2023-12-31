# Use a base image, e.g., Alpine for lightweight storage
FROM alpine

# Set a working directory
WORKDIR /app


# Copy files into the image
COPY ./dags /app/dags
