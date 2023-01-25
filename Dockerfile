FROM jupyter/pyspark-notebook

COPY . .
RUN rm -r work

RUN pip3 install -r requirements.txt

CMD ["jupyter-lab"]