FROM python as base

RUN mkdir /quota/
WORKDIR /quota

COPY requirements.txt /quota/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /quota/

ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","quota.py" ]

### PRODUCTION ###

FROM base as production
COPY . /quota/
ENTRYPOINT [ "python", "quota.py" ]
