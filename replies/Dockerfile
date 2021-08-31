FROM python as base

RUN mkdir /replies/
RUN mkdir /replies/utils
WORKDIR /replies

COPY requirements.txt /replies/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /replies/
ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","replies.py" ]

### PRODUCTION ###

FROM base as production
COPY . /replies/
ENTRYPOINT [ "python", "replies.py" ]
