# Build context is the repository root (see admin/compose.yml); only the files
# allowed by admin/Dockerfile.dockerignore reach the image.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    SGW_ADMIN_ENV=production \
    SGW_ADMIN_DB=/data/sgw-admin.db

RUN useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin sgw \
 && mkdir /data && chown sgw /data

WORKDIR /app
COPY admin/requirements.txt admin/requirements.txt
RUN pip install --no-cache-dir -r admin/requirements.txt
COPY custom_events.py ./
COPY admin ./admin
RUN install -m 0755 admin/bin/sgw-admin /usr/local/bin/sgw-admin \
 && python -m compileall -q /app

USER sgw
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os, sys, urllib.request; r = urllib.request.Request('http://127.0.0.1:8000/healthz', headers={'Host': os.environ.get('SGW_ADMIN_HOST', 'sgw-admin.srv1136792.hstgr.cloud'), 'X-Forwarded-Proto': 'https'}); sys.exit(0 if urllib.request.urlopen(r, timeout=3).status == 200 else 1)"
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "2", "--timeout", "90", "--graceful-timeout", "30", "admin.wsgi:app"]
