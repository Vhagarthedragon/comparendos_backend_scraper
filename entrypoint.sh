#!/bin/sh
pip install mysqlclient
cd infraction_core/
python manage.py migrate --no-input
python manage.py collectstatic --no-input
python manage.py makemigrations --no-input
python manage.py runserver 0.0.0.0:8800
exec "$@"