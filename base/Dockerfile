ARG STACK_TAG=d_latest
FROM ghcr.io/lsst/quick-stack:d_2024_06_21_DM-44996-8.0.0
ENV PYTHONUNBUFFERED True
RUN source /opt/lsst/software/stack/loadLSST.bash \
    && mamba install -y \
        flask \
        gunicorn \
        python-confluent-kafka \
    && pip install --no-input \
        cloudevents
