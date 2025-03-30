FROM python:3.13.2-slim-bullseye

RUN mkdir /opt/app
WORKDIR /opt/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg \
    unixodbc-dev \
    unixodbc \
    libpq-dev \
    curl \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools unixodbc-dev \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m client
USER client

CMD [ "python", "main.py" ]