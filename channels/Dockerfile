FROM python as base

RUN mkdir /channels/
RUN mkdir /channels/utils
WORKDIR /channels

COPY requirements.txt /channels/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
EXPOSE 8000
### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /channels/
EXPOSE 5678
ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","channels.py" ]

### PRODUCTION ###

FROM base as production
COPY . /channels/
ENTRYPOINT [ "python", "channels.py" ]
