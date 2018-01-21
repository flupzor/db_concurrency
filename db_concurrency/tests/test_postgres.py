import logging
import os

from django.contrib.auth.models import User
from django.db import transaction
from django.db.utils import OperationalError

from djchoices import ChoiceItem

from .base import ConcurrencyTests
from .utils import (
    OperationModes, WorkerStates, create_db_worker, receive_message,
    send_message
)

logger = logging.getLogger(__name__)


class PhantomReadTests(ConcurrencyTests):
    class WorkerStates(WorkerStates):
        transaction_started = ChoiceItem('transaction_started')
        retrieved_users = ChoiceItem('retrieved_users')
        created_user = ChoiceItem('created_user')
        transaction_commit = ChoiceItem('transaction_commit')

    def setUpWorkers(self):
        self.pid1, self.control1 = self.start_worker('user1')
        self.pid2, self.control2 = self.start_worker('user2')

    def setUp(self):
        super().setUp()

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.setup_worker))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.setup_worker))

    def tearDown(self):
        super().tearDown()

        os.waitpid(self.pid1, 0)
        self.control1.close()
        os.waitpid(self.pid2, 0)
        self.control2.close()

    @create_db_worker()
    def start_worker(self, username, worker_socket):
        command = receive_message(worker_socket)

        using = getattr(OperationModes, command)
        logger.debug('%s: pid: %d switching to isolation level %s', self, os.getpid(), using)

        send_message(worker_socket, True)

        command = receive_message(worker_socket)

        logger.debug('%s: pid: %d transaction_started', self, os.getpid())
        assert command == self.WorkerStates.transaction_started

        with transaction.atomic(using=using):
            send_message(worker_socket, True)

            command = receive_message(worker_socket)

            logger.debug('%s: pid: %d retrieved users', self, os.getpid())
            assert command == self.WorkerStates.retrieved_users
            qs = User.objects.using(using).all()
            users = list(qs)

            send_message(worker_socket, True)

            command = receive_message(worker_socket)

            logger.debug('%s: pid: %d created_user', self, os.getpid())
            assert command == self.WorkerStates.created_user
            success = False
            if len(users) <= 1:
                try:
                    User.objects.using(using).create(username=username)
                except OperationalError:
                    pass
                else:
                    success = True

            send_message(worker_socket, success)

            command = receive_message(worker_socket)

            logger.debug('%s: pid: %d transaction_commit', self, os.getpid())
            assert command == self.WorkerStates.transaction_commit

        send_message(worker_socket, True)

    def test_read_uncommitted(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_uncommitted))
        self.assertTrue(self.worker_to_state(self.control2, OperationModes.read_uncommitted))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.retrieved_users))

        # Now a second clients comes in
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.retrieved_users))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_commit))

        # and done
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 2)

    def test_read_committed(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_committed))
        self.assertTrue(self.worker_to_state(self.control2, OperationModes.read_committed))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.retrieved_users))

        # Now a second clients comes in
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.retrieved_users))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_commit))

        # and done
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 2)

    def test_repeatable_read(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.repeatable_read))
        self.assertTrue(self.worker_to_state(self.control2, OperationModes.repeatable_read))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.retrieved_users))

        # Now a second clients comes in
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.retrieved_users))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_commit))

        # and done
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 2)

    def test_serializable(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.serializable))
        self.assertTrue(self.worker_to_state(self.control2, OperationModes.serializable))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.retrieved_users))

        # Now a second clients comes in
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.retrieved_users))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.created_user))
        self.assertTrue(self.worker_to_state(self.control2, self.WorkerStates.transaction_commit))

        # trying to finish the already-started transaction on worker 1 will fail
        # because the other transaction modified the retrieved queryset.
        self.assertFalse(self.worker_to_state(self.control1, self.WorkerStates.created_user))

        # Transaction does, however, sucesfully finish, since the OperationalError was caught.
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.using('serializable').count(), 1)


class NonrepeatableReadTests(ConcurrencyTests):
    class WorkerStates(WorkerStates):
        transaction_started = ChoiceItem('ts')
        retrieve1 = ChoiceItem('r1')
        retrieve2 = ChoiceItem('r2')
        transaction_commit = ChoiceItem('tc')

    def setUpWorkers(self):
        self.pid1, self.control1 = self.start_worker()

    def setUp(self):
        super().setUp()
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.setup_worker))

    def tearDown(self):
        super().tearDown()
        os.waitpid(self.pid1, 0)

    @create_db_worker()
    def start_worker(self, worker_socket):
        command = receive_message(worker_socket)
        using = getattr(OperationModes, command)
        logger.debug('%s pid: %d switching to isolation level %s', self, os.getpid(), using)
        send_message(worker_socket, True)

        command = receive_message(worker_socket)
        logger.debug('%s pid: %d transaction_started', self, os.getpid())
        assert command == self.WorkerStates.transaction_started

        with transaction.atomic(using=using):
            send_message(worker_socket, True)
            command = receive_message(worker_socket)
            assert command == self.WorkerStates.retrieve1

            count1 = User.objects.using(using).count()
            send_message(worker_socket, count1)

            command = receive_message(worker_socket)
            assert command == self.WorkerStates.retrieve2

            count2 = User.objects.using(using).count()
            send_message(worker_socket, count2)

            command = receive_message(worker_socket)
            logger.debug('%s pid: %d transaction_commit', self, os.getpid())
            assert command == self.WorkerStates.transaction_commit
        send_message(worker_socket, True)

    def test_read_committed(self):
        operation_mode = OperationModes.read_committed
        using = getattr(OperationModes, operation_mode)
        self.assertTrue(self.worker_to_state(self.control1, operation_mode))

        User.objects.using(using).create(username='user1')
        User.objects.using(using).create(username='user2')

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))

        count1 = self.worker_to_state(self.control1, self.WorkerStates.retrieve1)
        self.assertEqual(count1, 2)

        User.objects.using(using).create(username='user3')

        count2 = self.worker_to_state(self.control1, self.WorkerStates.retrieve2)
        self.assertEqual(count2, 3)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

    def test_read_uncommitted(self):
        operation_mode = OperationModes.read_uncommitted
        using = getattr(OperationModes, operation_mode)
        self.assertTrue(self.worker_to_state(self.control1, operation_mode))

        User.objects.using(using).create(username='user1')
        User.objects.using(using).create(username='user2')

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))

        count1 = self.worker_to_state(self.control1, self.WorkerStates.retrieve1)
        self.assertEqual(count1, 2)

        User.objects.using(using).create(username='user3')

        count2 = self.worker_to_state(self.control1, self.WorkerStates.retrieve2)
        self.assertEqual(count2, 3)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

    def test_repeatable_read(self):
        operation_mode = OperationModes.repeatable_read
        using = getattr(OperationModes, operation_mode)
        self.assertTrue(self.worker_to_state(self.control1, operation_mode))

        User.objects.using(using).create(username='user1')
        User.objects.using(using).create(username='user2')

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))

        count1 = self.worker_to_state(self.control1, self.WorkerStates.retrieve1)
        self.assertEqual(count1, 2)

        User.objects.using(using).create(username='user3')

        count2 = self.worker_to_state(self.control1, self.WorkerStates.retrieve2)
        self.assertEqual(count2, 2)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

    def test_serializable(self):
        operation_mode = OperationModes.serializable
        using = getattr(OperationModes, operation_mode)
        self.assertTrue(self.worker_to_state(self.control1, operation_mode))

        User.objects.using(using).create(username='user1')
        User.objects.using(using).create(username='user2')

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))

        count1 = self.worker_to_state(self.control1, self.WorkerStates.retrieve1)
        self.assertEqual(count1, 2)

        User.objects.using(using).create(username='user3')

        count2 = self.worker_to_state(self.control1, self.WorkerStates.retrieve2)
        self.assertEqual(count2, 2)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))


class DirtyReadTests(ConcurrencyTests):
    class WorkerStates(WorkerStates):
        transaction_started = ChoiceItem('transaction_started')
        create_user = ChoiceItem('create_user')
        transaction_commit = ChoiceItem('transaction_commit')

    def setUpWorkers(self):
        self.pid1, self.control1 = self.start_worker()

    def tearDown(self):
        super().tearDown()

        os.waitpid(self.pid1, 0)

    def setUp(self):
        super().setUp()
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.setup_worker))

    @create_db_worker()
    def start_worker(self, worker_socket):
        command = receive_message(worker_socket)
        using = getattr(OperationModes, command)
        logger.debug('%s pid: %d switching to isolation level %s', self, os.getpid(), using)
        send_message(worker_socket, True)

        command = receive_message(worker_socket)
        logger.debug('%s pid: %d transaction_started', self, os.getpid())
        assert command == self.WorkerStates.transaction_started

        with transaction.atomic(using=using):
            send_message(worker_socket, True)

            command = receive_message(worker_socket)
            logger.debug('%s pid: %d create_user', self, os.getpid())
            assert command == self.WorkerStates.create_user

            User.objects.using(using).create(username='user1')
            send_message(worker_socket, True)

            command = receive_message(worker_socket)
            logger.debug('%s pid: %d transaction_commit', self, os.getpid())
            assert command == self.WorkerStates.transaction_commit
        send_message(worker_socket, True)

    def test_read_uncommitted(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_uncommitted))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.create_user))

        # In PostgreSQL read uncommitted behaves as read comitted. Dirty reads aren't possible.
        self.assertEquals(User.objects.using(OperationModes.read_uncommitted).count(), 0)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 1)

    def test_read_committed(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_committed))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.create_user))

        self.assertEquals(User.objects.using(OperationModes.read_uncommitted).count(), 0)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 1)

    def test_repeatable_read(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_committed))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.create_user))

        self.assertEquals(User.objects.using(OperationModes.read_uncommitted).count(), 0)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 1)

    def test_serializable(self):
        self.assertTrue(self.worker_to_state(self.control1, OperationModes.read_committed))

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_started))
        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.create_user))

        self.assertEquals(User.objects.using(OperationModes.read_uncommitted).count(), 0)

        self.assertTrue(self.worker_to_state(self.control1, self.WorkerStates.transaction_commit))

        self.assertEquals(User.objects.count(), 1)
