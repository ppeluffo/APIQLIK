FROM ubuntu:20.04

COPY Requirements.txt .

RUN apt-get update && apt-get install -y --no-install-recommends python3.8 python3-pip libpq-dev gcc tzdata python3.8-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade wheel --upgrade pip && pip install --no-cache-dir -r Requirements.txt \
    && apt-get purge -y --auto-remove gcc python3.8-dev

RUN cp /usr/share/zoneinfo/America/Montevideo /etc/localtime
RUN echo "America/Montevideo" >  /etc/timezone
RUN date
