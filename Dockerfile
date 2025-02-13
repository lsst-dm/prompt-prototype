ARG BASE_TAG=latest
FROM ghcr.io/lsst-dm/prompt-base:${BASE_TAG}
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
ENV PROMPT_PROCESSING_DIR=$APP_HOME
ARG PORT
WORKDIR $APP_HOME
COPY python/activator activator/
# Initializer is not run by this container, but its code is needed to generate consistent version hashes.
COPY python/initializer initializer/
COPY python/shared shared/
COPY pipelines pipelines/
COPY config/gunicorn.conf.py ./
COPY maps maps/
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && exec gunicorn --bind :$PORT --config gunicorn.conf.py 'activator.activator:create_app()'
