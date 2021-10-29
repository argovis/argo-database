1. Download ftp://kakapo.ucsd.edu/pub/gilson/argo_climatology/RG_ArgoClim_Temperature_2019.nc.gz to `raw/.`
2. docker image build -t dataloader .
3. docker network create apidev
4. docker container run -p 127.0.0.1:27017:27017 --network apidev -v $(pwd)/db:/data/db -d --name database mongo:4.2.3
5. docker container run -it --network apidev -v $(pwd)/raw:/raw dataloader bash
6. cd grids ; python loadgrid.py

One document will be loaded in argo.rgTempMean
