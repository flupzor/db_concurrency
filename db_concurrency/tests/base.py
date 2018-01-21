import logging
import os

from django.test import TransactionTestCase

from .utils import receive_message, send_message

logger = logging.getLogger('tests')


class ConcurrencyTests(TransactionTestCase):

    def __init__(self, *args, **kwargs):
        """
        __init__ is being used here, instead of setUp() because setUp()
        is called within a try except statement, and the fork()ed workers
        need to be able to raise SystemExit() to be able to quit.

        Also a note on how a testsuite is run, the loader looks through
        all test names and finds the ones starting with ``test_`` (getTestCaseNames)
        then for every testmethod found, a new TestCase is instantiated. That's
        why this __init__ method is instantiated for every test, and also the workers
        are started for every test.
        """
        super().__init__(*args, **kwargs)

        self.setUpWorkers()

    def setUpWorkers(self):
        raise NotImplemented

    def worker_to_state(self, control_socket, state):
        """
        Move a specified worker to the given state.

        :param control_socket fd of the socket to communicate with.
        :param state state to move to.
        :return the value returned by the worker
        """
        send_message(control_socket, state)
        return receive_message(control_socket)
