FROM python:3.13.7-alpine3.22 AS pa-python-odbc

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apk update \
    && apk add unixodbc \
    && apk add --no-cache --virtual .build-deps curl gnupg g++ musl-dev unixodbc-dev \
    && curl -O https://download.microsoft.com/download/fae28b9a-d880-42fd-9b98-d779f0fdd77f/msodbcsql18_18.5.1.1-1_amd64.apk \
    && curl -O https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_amd64.apk \
    && curl -O https://download.microsoft.com/download/fae28b9a-d880-42fd-9b98-d779f0fdd77f/msodbcsql18_18.5.1.1-1_amd64.sig \
    && curl -O https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_amd64.sig \
    && curl https://packages.microsoft.com/keys/microsoft.asc  | gpg --import - \
    && gpg --verify msodbcsql18_18.5.1.1-1_amd64.sig msodbcsql18_18.5.1.1-1_amd64.apk \ 
    && gpg --verify mssql-tools18_18.4.1.1-1_amd64.sig mssql-tools18_18.4.1.1-1_amd64.apk \
    && apk add --allow-untrusted msodbcsql18_18.5.1.1-1_amd64.apk \
    && apk add --allow-untrusted mssql-tools18_18.4.1.1-1_amd64.apk \
    && pip install --upgrade pip \
    && pip install --no-cache-dir pyodbc \
    && apk del .build-deps curl gnupg \
    && ln -s /opt/mssql-tools/bin/sqlcmd /usr/local/bin/sqlcmd \
    && ln -s /opt/mssql-tools/bin/bcp /usr/local/bin/bcp 

RUN mkdir -p /app
WORKDIR /app

# Non-root user
RUN addgroup -S app && adduser -S -h /app -s /usr/sbin/nologin app \
 && chown -R app:app /app

 ####################################################################
 # Image customization from here
 ####################################################################
FROM pa-python-odbc

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY --chown=app:app . .

USER app

CMD ["python", "main.py"]