ARG BIGQUERY_SERVICE_ACCOUNT
FROM python:3.8
MAINTAINER "connor.charles@autotrader.co.uk"

WORKDIR /usr/local/dry-run/

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry
    # https://python-poetry.org/docs/configuration/#using-environment-variables
    POETRY_VERSION=1.1.11 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    \
    # paths
    # this is where our requirements + virtual environment will live
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

#RUN apk --no-cache add curl build-base libffi-dev musl-dev openssl-dev cargo python3-dev
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python - --no-modify-path
RUN poetry config virtualenvs.create false

COPY poetry.lock /usr/local/dry-run/
COPY pyproject.toml /usr/local/dry-run/

RUN poetry install

COPY /dbt_dry_run /usr/local/dry-run/dbt_dry_run
COPY /integration /usr/local/dry-run/integration

RUN echo $BIGQUERY_SERVICE_ACCOUNT > /etc/bq_svc.json
RUN /usr/local/dry-run/integration/integration-test.sh