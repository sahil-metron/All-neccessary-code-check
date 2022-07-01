# syntax=docker/dockerfile:1
FROM devo.com/ifc_base:1.0

WORKDIR /devo-collector

COPY dockerfiles/.bashrc /root/.bashrc

RUN \
### Adding user LOGTRUST
    addgroup --gid 1000 logtrust &&  \
    adduser --system --shell /bin/bash --gid 1000 --uid 1000 --disabled-password logtrust &&  \
    mkdir -p /home/logtrust/ &&  \
    cp /root/.bashrc /home/logtrust/.bashrc &&  \
    chown -R logtrust:logtrust /home/logtrust &&  \
    usermod -aG sudo logtrust &&  \
    usermod -c "Logtrust main user" logtrust && \
### Adding user DEVO, password "devo"
    addgroup --gid 1001 devo &&  \
    adduser --system --shell /bin/bash --gid 1001 --uid 1001 --disabled-password devo &&  \
    mkdir -p /home/devo/ &&  \
    cp /root/.bashrc /home/devo/.bashrc &&  \
    chown -R devo:devo /home/devo &&  \
    usermod -aG sudo devo &&  \
    usermod -c "Devo main user" devo && \
###
    usermod -aG devo logtrust && \
    usermod -aG logtrust devo

RUN pip --default-timeout=10 install --upgrade pip

ADD requirements.txt /devo-collector/
RUN pip --default-timeout=10 install -r ./requirements.txt && \
    rm -rf /devo-collector/requirements.txt

COPY agent /devo-collector/devo-agent/agent
COPY __init__.py /devo-collector/devo-agent/
COPY setup.cfg /devo-collector/devo-agent/
COPY setup.py /devo-collector/devo-agent/
COPY README*.md /devo-collector/devo-agent/
RUN pip --default-timeout=10 install /devo-collector/devo-agent && \
    rm -rf /devo-collector/devo-agent

COPY metadata.json /devo-collector
RUN mkdir -p /devo-collector/config; \
    mkdir -p /devo-collector/config_internal

COPY config_internal/collector_definitions.yaml /devo-collector/config_internal/
RUN mkdir -p /devo-collector/certs; \
    mkdir -p /devo-collector/credentials; \
    mkdir -p /devo-collector/state; \
    mkdir -p /etc/devo/job; \
    mkdir -p /etc/devo/collector

#RUN chown -R devo:devo /devo-collector
#RUN chown -R devo:devo /etc/devo/
#
#USER devo

RUN date +%Y-%m-%dT%H:%M:%S.%N%z > build_time.txt

ENTRYPOINT devo-collector --config ${CONFIG_FILE:-config.yaml}
