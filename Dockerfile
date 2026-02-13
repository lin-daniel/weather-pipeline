# 1. Base Image: Start with a lightweight version of Python 3.9
FROM python:3.9-slim

# 2. Set the working directory inside the container to /app
WORKDIR /app

# 3. Copy requirements first (for better caching)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your code and your key into the container
COPY extract.py .
COPY service-account.json .

# 6. Set the Environment Variable
# This tells Google's library EXACTLY where to look for the key inside the container
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json"

# 7. The command to run when the container starts
CMD ["python", "extract.py"]