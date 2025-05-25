FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc libglib2.0-0 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Git, SSH client, wget, and OpenJDK
RUN apt-get update && apt-get install -y \
    git \
    openssh-client \
    wget \
 && rm -rf /var/lib/apt/lists/*

# Add GitHub to known_hosts so SSH connections can verify the host key
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN --mount=type=ssh pip install --upgrade pip && pip install -r requirements.txt

COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["python", "-m", "main.main"]
