FROM python:3.9.16-slim-buster as base

ARG PYPI_USERNAME
ENV PYPI_USERNAME=$PYPI_USERNAME

ARG PYPI_PASSWORD
ENV PYPI_PASSWORD=$PYPI_PASSWORD

# installing poetry

RUN apt-get update && \
    apt-get install -y -q build-essential curl
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VERSION=1.3.2
RUN curl -sSL https://install.python-poetry.org | python3 -

# copying package files
COPY . ./

# publishing package
RUN poetry build

# for test PyPI index (local development)
#RUN poetry config repositories.testpypi https://test.pypi.org/legacy/
#RUN poetry publish --repository testpypi --username $PYPI_USERNAME --password $PYPI_PASSWORD

# for real PyPI index
RUN poetry publish --username $PYPI_USERNAME --password $PYPI_PASSWORD