"""gunicorn entry point: `gunicorn admin.wsgi:app`."""

from admin.app import create_app

app = create_app()
