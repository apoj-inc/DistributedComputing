FROM ubuntu:24.04 AS worker

ENV MAIN=/home/main
WORKDIR $MAIN

COPY build/x86_64-linux/src/worker/dc_worker ./worker
RUN chmod +x ./worker

ENTRYPOINT ["./worker"]

FROM ubuntu:24.04 AS master

ENV DEBIAN_FRONTEND=noninteractive
ENV MAIN=/home/main
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR $MAIN

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        python3-dev \
        build-essential \
        libpq-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY src/master/requirements.txt requirements.txt
RUN python3 -m venv "${VIRTUAL_ENV}" \
    && pip install --no-cache-dir -r requirements.txt

COPY build/x86_64-linux/src/master/dc_master ./master
RUN chmod +x ./master

COPY scripts/init_pg.py .
COPY scripts/init_mongo.py .

ENTRYPOINT ["./master"]

FROM ubuntu:24.04 AS cli

ENV MAIN=/home/main
WORKDIR $MAIN

COPY build/x86_64-linux/src/cli/dc_cli ./cli
RUN chmod +x ./cli

ENTRYPOINT ["./cli"]
