FROM python as base

RUN mkdir /include/
WORKDIR /include

COPY requirements.txt /include/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /include/

ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","include.py" ]

### PRODUCTION ###

FROM base as production
COPY . /include/
ENTRYPOINT [ "python", "include.py" ]
