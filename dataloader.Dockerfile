FROM spymovil/apiqlik_imagen_base:latest

WORKDIR /apiqlik
RUN mkdir bdatos

COPY ./dataloader/*py ./
COPY ./bdatos/*.py bdatos 
RUN chmod 777 /apiqlik/* -R

CMD ["python3", "/apiqlik/apiqlik_dataloader.py"]
