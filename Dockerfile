FROM python:3.7

RUN pip install numpy pandas xarray pymongo scipy

COPY grids /grids