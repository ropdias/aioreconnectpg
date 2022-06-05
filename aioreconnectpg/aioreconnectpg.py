import asyncio
import logging
import psycopg
from typing import Any

LOGGER = logging.getLogger(__name__)


class AsyncReconnectPsycopg:
    """
    This class connect to a PostgreSQL database using an AsyncConnection from psycopg and uses another AsyncConnection
    to detect any disconnect as recommended by psycopg itself:
    https://www.psycopg.org/psycopg3/docs/advanced/async.html#detecting-disconnections
    To start you should use a separate task to call and await run() on the instance of AsyncReconnectPsycopg.
    When you want to stop you can call and await .stop() on the instance of AsyncReconnectPsycopg.
    Before using the main connection with the attribute .aconn is advisable to acquire the attribute task_lock
    (an asyncio.Lock object) to avoid any race condition with the internal _detect_disconnect() task (You can have a
    [WinError 10038] if you are reading/writing while the _detect_disconnect() task try to close the connections and
    remove the fd reader)
    You can also use multiple tasks to use the same main connection but, again, is advisable to acquire the attribute
    task_lock before using it to avoid any race condition between tasks, especially if you are going to use transactions
    (or you could open nested transactions and get errors for trying to commit a transaction in the wrong nesting level)
    """
    def __init__(self, hostaddr: str,
                 port: str,
                 dbname: str,
                 user: str,
                 password: str,
                 autocommit: bool = False) -> None:
        self._conninfo = f'hostaddr={hostaddr} port={port} dbname={dbname} user={user} password={password}'
        self._autocommit = autocommit
        self._online = asyncio.Event()
        self._offline = asyncio.Event()
        self._offline.set()
        self._stopping = False
        self._aconn: psycopg.AsyncConnection[Any] | None = None
        self._aconn_to_detect_disconnect: psycopg.AsyncConnection[Any] | None = None
        self._aconn_to_detect_disconnect_fileno: int | None = None
        self._disconnect_event = asyncio.Event()
        self._disconnect_task: asyncio.Task | None = None
        self._loop = asyncio.get_event_loop()
        self._task_lock = asyncio.Lock()

    async def _detect_disconnect(self):
        # This event will trigger with file descriptor activity when connection is lost
        await self._disconnect_event.wait()
        # We will try to close connection so they will change state to "BAD"
        async with self._task_lock:  # Lock used to prevent a race condition
            await self._close_connection()

    async def _cancel_disconnect_task_and_wait(self):
        if self._disconnect_task is not None:
            if not self._disconnect_task.cancelled() and not self._disconnect_task.done():
                self._disconnect_task.cancel()
            # We need to wait for the cancellation to propagate:
            await asyncio.wait({self._disconnect_task})

    async def run(self) -> None:
        """ Wait the connection to go offline or to be asked to stop()"""
        while not self._stopping:
            try:
                async with self._task_lock:  # Lock used to prevent a race condition
                    await self._connect()
            except psycopg.Error as error:
                LOGGER.error(f'Error={error} while trying to connect to DB. Waiting 2 seconds for a retry.')
                await asyncio.sleep(2)
            await self._offline.wait()
        await self._cancel_disconnect_task_and_wait()
        LOGGER.info('Stopped run() from AsyncReconnectPsycopg after closing the connection.')

    async def stop(self) -> None:
        """Stop the example by closing the connection after setting self._stopping = True.
        """
        LOGGER.info('Closing DB Connection and stopping AsyncReconnectPsycopg.')
        self._stopping = True
        async with self._task_lock:  # Lock used to prevent a race condition
            await self._close_connection()

    async def _connect(self) -> None:
        try:
            LOGGER.info('Trying to connect to DB.')
            self._aconn = await psycopg.AsyncConnection.connect(conninfo=self._conninfo,
                                                                autocommit=self._autocommit)
            self._aconn_to_detect_disconnect = await psycopg.AsyncConnection.connect(conninfo=self._conninfo,
                                                                                     autocommit=self._autocommit)
            LOGGER.info('Connected to Database !')
        except psycopg.Error:
            self._online.clear()
            self._offline.set()
            raise
        else:
            self._aconn_to_detect_disconnect_fileno = self._aconn_to_detect_disconnect.fileno()
            self._online.set()
            self._offline.clear()
            self._disconnect_event.clear()
            self._loop.add_reader(self._aconn_to_detect_disconnect_fileno, self._disconnect_event.set)
            await self._cancel_disconnect_task_and_wait()  # To make sure to stop any task that could be running
            self._disconnect_task = asyncio.create_task(self._detect_disconnect())

    async def _close_connection(self) -> None:
        if self._aconn is not None and not self._aconn.closed:
            LOGGER.info(f'Closing: {self._aconn}')
            await self._aconn.close()
        if self._aconn_to_detect_disconnect is not None and not self._aconn_to_detect_disconnect.closed:
            # We need to remove reader before closing connection, otherwise a [WinError 10038] is raised.
            self._loop.remove_reader(self._aconn_to_detect_disconnect_fileno)
            LOGGER.info(f'Closing: {self._aconn_to_detect_disconnect}')
            await self._aconn_to_detect_disconnect.close()
        LOGGER.info(f'Database connection is closed.')
        self._online.clear()
        self._offline.set()

    @property
    def aconn(self) -> psycopg.AsyncConnection[Any] | None:
        return self._aconn

    @property
    def task_lock(self) -> asyncio.Lock:
        return self._task_lock

    @property
    def aconn_to_detect_disconnect(self) -> psycopg.AsyncConnection[Any] | None:
        return self._aconn_to_detect_disconnect

    # This method can be used to await the connection to be online before trying another query/transaction:
    async def wait_online(self) -> bool:
        return await self._online.wait()

    # This method can be used to await the connection to be offline
    async def wait_offline(self) -> bool:
        return await self._offline.wait()
