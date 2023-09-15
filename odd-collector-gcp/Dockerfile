FROM python:3.9.16-slim-buster as base
ENV POETRY_PATH=/etc/poetry \
    POETRY_VERSION=1.3.2
ENV PATH="$POETRY_PATH/bin:$VENV_PATH/bin:$PATH"

FROM base AS build

RUN apt-get update && \
    apt-get install -y -q build-essential \
    python3-dev  \
    curl

RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=${POETRY_PATH} python3 -
RUN poetry config virtualenvs.create false
RUN poetry config experimental.new-installer false

COPY poetry.lock pyproject.toml ./
RUN poetry install --no-interaction --no-ansi --no-dev -vvv


FROM base as runtime

RUN useradd --create-home --shell /bin/bash app
USER app

# non-interactive env vars https://bugs.launchpad.net/ubuntu/+source/ansible/+bug/1833013
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV UCF_FORCE_CONFOLD=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . ./
COPY --from=build /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

ENTRYPOINT ["bash", "start.sh"]
