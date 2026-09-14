FROM node:22-bookworm AS frontend

WORKDIR /build
COPY gflow-studio-prototype/package.json gflow-studio-prototype/package-lock.json ./
RUN npm ci
COPY gflow-studio-prototype/vite.config.js ./
COPY gflow-studio-prototype/react-app ./react-app
RUN npm run build

FROM python:3.11-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update \
    && apt-get install -y --no-install-recommends xvfb ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY gflow-cli /opt/gflow-cli
COPY gflow-studio-prototype /opt/gflow-studio-prototype

RUN pip install --no-cache-dir -e '/opt/gflow-cli[s3]' \
    && playwright install --with-deps chromium

COPY --from=frontend /build/dist /opt/gflow-studio-prototype/dist

WORKDIR /opt/gflow-studio-prototype
ENV PYTHONPATH=/opt/gflow-cli/src
