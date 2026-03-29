FROM ubuntu:22.04 AS worker

ENV MAIN=/home/main

COPY build/x86_64-linux/src/worker/dc_worker worker
RUN chmod +x worker

WORKDIR $MAIN

ENTRYPOINT ["worker"]

FROM ubuntu:22.04 AS master

ENV MAIN=/home/main

COPY build/x86_64-linux/src/master/dc_master master
RUN chmod +x master

WORKDIR $MAIN

ENTRYPOINT ["master"]

FROM ubuntu:22.04 AS cli

ENV MAIN=/home/main

COPY build/x86_64-linux/src/master/dc_cli cli
RUN chmod +x cli

WORKDIR $MAIN

ENTRYPOINT ["cli"]
