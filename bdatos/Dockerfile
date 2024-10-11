FROM timescale/timescaledb:latest-pg15

RUN apk add tzdata
RUN cp /usr/share/zoneinfo/America/Montevideo /etc/localtime
RUN echo "America/Montevideo" >  /etc/timezone
RUN date 
RUN apk del tzdata
COPY *.sql /docker-entrypoint-initdb.d/
