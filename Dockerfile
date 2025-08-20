FROM python:3.13.7-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN mkdir -p /opt/app
WORKDIR /opt/app

# System deps + MS ODBC 18 (Debian 12 / Bookworm), using signed keyring (no apt-key)
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      gnupg \
      unixodbc \
      unixodbc-dev \
      libpq-dev \
 && mkdir -p /usr/share/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/msprod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/msprod.gpg] https://packages.microsoft.com/repos/microsoft-debian-bookworm-prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools \
 # expose sqlcmd/bcp without editing shell rc files
 && ln -s /opt/mssql-tools/bin/sqlcmd /usr/local/bin/sqlcmd \
 && ln -s /opt/mssql-tools/bin/bcp /usr/local/bin/bcp \
 # cleanup
 && apt-get purge -y --auto-remove gnupg \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Non-root user (same name as your original)
RUN useradd -m -r -d /opt/app -s /usr/sbin/nologin client \
 && chown -R client:client /opt/app
USER client

CMD ["python", "main.py"]