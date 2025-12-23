FROM python:3.11.1-slim-bullseye

WORKDIR /app

# Install curl for healthchecks
RUN apt update && apt install -y curl

#build-base gfortran libopenblas-dev wget freetype-dev libpng-dev linux-headers libxml2-dev libxslt-dev

# Setup a nonroot user for security
# RUN adduser -D nonroot
# USER nonroot

# Upgrade pip
RUN pip install --upgrade pip

# Install dependencies (split into 2 sets, to shorten build times)
COPY requirements.txt .
RUN pip install --user --no-cache-dir --upgrade -r requirements.txt

# Expose the app's port
EXPOSE 8000

COPY src ./src

# Run the FastAPI server
ENTRYPOINT ["python", "-m"]
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
