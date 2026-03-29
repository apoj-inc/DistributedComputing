FROM ubuntu:22.04 AS worker

ENV MAIN=/home/main

COPY build/x86_64-linux/src/worker/dc_worker worker
RUN chmod +x worker

WORKDIR $MAIN

ENTRYPOINT ["worker"]
