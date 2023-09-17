FROM python:3.11.4-bullseye
LABEL maintainer="u6k.apps@gmail.com"

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/ && \
    poetry config virtualenvs.create false

# Install poetry packages
WORKDIR /var/myapp
VOLUME /var/myapp

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

CMD ["poetry", "help"]
