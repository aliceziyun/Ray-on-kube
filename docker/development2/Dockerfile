# development ray2, use local modified ray to run programs
FROM rayproject/ray:latest
RUN rm -rf /anaconda3/lib/python3.8/site-packages/ray
COPY ./ray /anaconda3/lib/python3.8/site-packages/

RUN pip install grpcio