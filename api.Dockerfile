FROM spymovil/apiqlik_imagen_base:latest

WORKDIR /apiqlik
RUN mkdir bdatos

COPY ../api/*py ./
COPY ../bdatos/*.py bdatos

COPY ../api/entrypoint.sh .
RUN chmod 777 /apiqlik/* -R
ENTRYPOINT ["sh", "entrypoint.sh"]

EXPOSE 5022

