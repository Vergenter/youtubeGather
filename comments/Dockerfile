FROM python as base

RUN mkdir /comments/
RUN mkdir /comments/utils
WORKDIR /comments

COPY requirements.txt /comments/requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

### DEBUG ###
FROM base as debug

RUN python -m pip install debugpy

COPY . /comments/
ENTRYPOINT [ "python", "-m", "debugpy","--wait-for-client", "--listen", "0.0.0.0:5678","comments.py" ]

### PRODUCTION ###

FROM base as production
COPY . /comments/
ENTRYPOINT [ "python", "comments.py" ]
