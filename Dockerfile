#  Start with the python environment
FROM python:3.10.8-slim

# Create the app directory in the container
RUN mkdir -p /opt/dagster/app

# Install the python dependencies
RUN pip install pandas dagit dagster joblib tqdm swifter pyarrow

# Copy your code and workspace to /opt/dagster/app
COPY . /opt/dagster/app/

# Set the environment variables
ENV DAGSTER_HOME=/opt/dagster/app/

# Set the working directory
WORKDIR /opt/dagster/app

# Expose the port for access to the dagit server
EXPOSE 3000

# Start the dagit server
ENTRYPOINT ["dagit", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]