import json
import logging
import os
import socket
import sys
from functools import wraps
from unittest import TestCase

from django.test.utils import setup_databases

from djchoices import ChoiceItem, DjangoChoices

logger = logging.getLogger(__name__)


class OperationModes(DjangoChoices):
    read_committed = ChoiceItem('read_committed')
    read_uncommitted = ChoiceItem('read_uncommitted')
    repeatable_read = ChoiceItem('repeatable_read')
    serializable = ChoiceItem('serializable')


class WorkerStates(DjangoChoices):
    setup_worker = ChoiceItem('setup_worker')


MAX_BUFFER_SIZE = 4096


def send_message(sock, value):
    data = json.dumps(value).encode('utf-8')
    assert len(data) <= MAX_BUFFER_SIZE
    sock.sendall(data)


def receive_message(sock):
    data = sock.recv(MAX_BUFFER_SIZE)
    return json.loads(data)


class WorkerDecorator:
    def before(self):
        raise NotImplementedError

    def after(self):
        raise NotImplementedError

    def __init__(self, kwarg_name=None):
        self.kwarg_name = kwarg_name

    def __enter__(self):
        # Note: Returning self means that in "with ... as x", x will be self
        return self

    def __exit__(self, typ, val, traceback):
        pass

    @staticmethod
    def get_testinfo(*args, **kwargs):
        """
        A crude way to get some diagnostic info on which TestCase
        started this worker.
        """
        if len(args) >= 1 and isinstance(args[0], TestCase):
            return str(args[0])
        return 'Unknown test'

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self as context:
                testinfo = self.get_testinfo(*args, **kwargs)

                if self.kwarg_name:
                    kwargs[self.kwarg_name] = context
                control_socket, worker_socket = socket.socketpair()

                pid = os.fork()

                # If a lot of tests are run, the worker might have to wait
                # a while. So don't set a time-out on the worker socket. Only
                # on the control socket.
                control_socket.settimeout(100)

                if (pid != 0):
                    worker_socket.close()
                    return pid, control_socket

                logger.debug(
                    "%s: forked worker pid: %d control socket: %d worker socket: %d",
                    testinfo, os.getpid(), control_socket.fileno(),
                    worker_socket.fileno())

                control_socket.close()

                self.before(worker_socket, args)

                new_args = list(args) + [worker_socket, ]

                try:
                    func(*new_args, **kwargs)
                except Exception:
                    pass

                self.after(worker_socket)

                sys.exit()

        return wrapper


class create_db_worker(WorkerDecorator):
    def before(self, worker_socket, args):
        command = receive_message(worker_socket)

        if command != WorkerStates.setup_worker:
            send_message(worker_socket, False)
            return

        # A bit of trickery to use the test database in the forked process.
        setup_databases(verbosity=False, interactive=False, keepdb=True)

        send_message(worker_socket, True)

    def after(self, worker_socket):
        pass


class create_worker(WorkerDecorator):
    def before(self, worker_socket):
        pass

    def after(self, worker_socket):
        pass
