FROM python as base

RUN mkdir /videos/
RUN mkdir /videos/utils
WORKDIR /videos

COPY requirements.txt /videos/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
EXPOSE 8000
### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /videos/
EXPOSE 5678
ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","videos.py" ]

### PRODUCTION ###

FROM base as production
COPY . /videos/
ENTRYPOINT [ "python", "videos.py" ]
