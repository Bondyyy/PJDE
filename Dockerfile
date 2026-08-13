FROM python:3.12-slim-bookworm
# PYTHON SETTINGS

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# INSTALL JAVA FOR PYSPARK
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# JAVA ENVIRONMENT
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# PROJECT DIRECTORY INSIDE CONTAINER
WORKDIR /app

# INSTALL PYTHON DEPENDENCIES
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# COPY PROJECT SOURCE CODE
COPY . .

CMD ["python", "-m", "src.main"]