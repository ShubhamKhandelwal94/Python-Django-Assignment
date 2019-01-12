"""
WSGI config for PythonDjangoAssignment project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/howto/deployment/wsgi/
"""
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from assignment.views import TransactionList
import os
from threading import Timer, Thread, Event
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'PythonDjangoAssignment.settings')

application = get_wsgi_application()



print (application)

