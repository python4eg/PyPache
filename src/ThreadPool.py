import logging
import Queue
import threading
import time

TP_SLEEP_TIME = 5
ST_SLEEP_TIME = 0.5
PUT_TIMEOUT = 2
GET_TIMEOUT = 2
JOIN_TIMEOUT = 0.1


class PoolException(Exception):

    """PoolException, raised any exception."""

    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message


class PoolEmpty(PoolException):

    """PoolEmpty, raised exception if Queue is Empty."""

    def __init__(self, val):
        self.message = 'ThreadPool is Empty. %s' % (val,)
    def __str__(self):
        return self.message


class PoolFull(PoolException):

    """PoolFull, raised exception if Queue is Full."""

    def __init__(self, val):
        self.message = 'ThreadPool is Full. %s' % (val,)
    def __str__(self):
        return self.message


class PoolCallError(PoolException):

    """PoolCallError, raised exception if task is not callable"""

    def __init__(self, val):
        self.message = 'Call Error:<%s> object is not callable' % (val,)
    def __str__(self):
        return self.message


class ThreadPool:
    """ThreadPool. Pool thread for processing tasks"""

    def __init__(self, task, max_workers, max_qsize, max_idle_time, log=None):

        """Initialize a ThreadPool object.

        Parameters:
            - `task`: task to handled.
            - `max_workers`: max count workers.
            - `max_qsize`: maximum size of queue
            - `max_idle_time`: max idle time for workers
            - `log`: take logger

        :Exceptions:
            - `PoolCallError`: raise if task not callable
        """

        if log:
            self.logger = log
        else:
            self.logger = logging.getLogger('ThreadPool')

        if not callable(task):
            self.logger.error('Call Error:<%s> object is not callable', task.__name__)
            raise PoolCallError(task.__name__)

        self.__task = task
        self.__workers = []
        self.__max_workers = max_workers
        self.__max_queue_size = max_qsize
        self.__max_idle_time = max_idle_time
        self.__task_lock = threading.RLock()
        self.__tasks = Queue.Queue(max_qsize)
        self.__init_housekeeper()

    def __init_worker(self):
        new_worker = Worker(self)
        self.__workers.append(new_worker)
        new_worker.start()

    def __init_housekeeper (self):
        self.logger.debug('Init Housekeeper')
        self.__housekeeper = Housekeeper(self, self.__max_idle_time)
        self.__housekeeper.start()

    def add_task(self, *args, **kwargs):

        """add_task. Method for adding task with any args or without them to queue.

        :Exceptions:
            - `PoolFull`: raise if queue is Full
            - `PoolAddTaskError`: raise if there are no workers, to handle tasks
        """

        try:
            if len(self.__workers) <= self.__max_workers:
                self.__init_worker()
            self.__tasks.put((self.__task, args, kwargs), timeout=PUT_TIMEOUT)
            self.logger.debug('Add Task')
        except Queue.Full:
            raise PoolFull('Wait timeout')
        except Exception, e:
            self.logger.error(e)

    def get_task(self):

        """get_task. Method for getting task form queue.

        :Exceptions:
            - `PoolEmpty`: raise if queue is Empty

        :Return:
            (func or class, args <list>, kwargs <dict>)
        """

        try:
            next_task = self.__tasks.get(timeout=GET_TIMEOUT)
            self.logger.debug('Get Next Task')
        except Queue.Empty:
            raise PoolEmpty('Wait timeout')
        except Exception, e:
            self.logger.error(e)
        else:
            return next_task

    def get_workers(self):

        """get_workers. Return list of workers"""

        return self.__workers

    def get_qsize(self):

        """get_qsize. Return size queue."""

        return self.__tasks.qsize()

    def drop(self, worker):

        """drop. Stop worker, and remove them from list

        :Parameters:
            - `worker`: object class Workers
        """

        if isinstance(worker, Worker) and worker in self.__workers:
            worker.stop()
            self.__workers.remove(worker)

    def wait_until_done(self):

        """wait_until_done. Wait until queue of task will be empty"""

        self.logger.debug('Wait until done')
        while True:
            try:
                temp_data = self.__tasks.get_nowait()
                self.__tasks.put_nowait(temp_data)
                time.sleep(TP_SLEEP_TIME)
            except Queue.Empty, e:
                break
        return True

    def stop_all(self):

        """stop_all. Stop all workers"""

        self.__task_lock.acquire()
        try:
            self.logger.debug('Stop all')
            self.__housekeeper.stop()
            while self.__workers:
                for worker in self.__workers:
                    worker.stop()
                    self.__workers.remove(worker)
        finally:
            self.__task_lock.release()


class Worker(threading.Thread):
    __id = 0

    def __init__(self, pool):

        """Initialize a Workers object.

        Parameters:
            - `pool`: Threadpool.
        """

        threading.Thread.__init__(self)
        self.__pool = pool
        Worker.__id += 1
        self.__id = Worker.__id
        self.setName('Worker-%s' % (self.__id,))
        self.__is_alive = True
        self.__idle_time = 0

    def run(self):

        """Reimplementation run method from threading.Thread.
        Gets and executes tasks from queue
        """
        while self.__is_alive:
            try:
                self.__idle_time = time.time()
                ts = self.__pool.get_task()
                self.__idle_time = 0
                task, args, kwargs = ts
                self.__pool.logger.debug('Start worker: %s, with connection from %s' %
                                        (self.get_name(), args[0][1],))
                task(*args)
                self.__pool.logger.debug('Complited')
            except PoolEmpty, e:
                self.__pool.logger.debug(e)
            except Exception, e:
                self.__pool.logger.error(e)

    def get_idle_time(self):

        """get_idle_time. Return idle time from current worker"""

        if self.__idle_time:
            idle = time.time() - self.__idle_time
        else:
            idle = self.__idle_time
        return idle

    def get_name(self):

        """get_name. Return name from current worker"""

        return self.getName()

    def stop(self):

        """stop. Stop current worker"""

        self.__pool.logger.debug('Stop worker: %s', self.get_name())
        self.__is_alive = False
        self.join(JOIN_TIMEOUT)


class Housekeeper(threading.Thread):

    """Housekeeper
    Thread handle idle time all workers.
    """

    def __init__(self, pool, max_idle_time):

        """Initialize a Housekeeper object.

        Parameters:
            - `pool`: Threadpool.
            - `max_idle_time`: max idle time for all worker
        """

        threading.Thread.__init__(self)
        self.__pool = pool
        self.__max_idle_time = max_idle_time
        self.__is_alive = True

    def run(self):

        """Reimplantation run method from threading.Thread.
        Check idle time worker, and if idle time > max idle time,
        drop this worker.
        """

        self.__pool.logger.debug('Start Housekeeper thread')
        while self.__is_alive:
            time.sleep(ST_SLEEP_TIME)
            for worker in self.__pool.get_workers():
                if worker.get_idle_time() >= self.__max_idle_time:
                    self.__pool.logger.debug('Worker %s timeout. Drop', worker.get_name())
                    self.__pool.drop(worker)

    def stop(self):

        """stop. Stop Housekeeper"""

        self.__pool.logger.debug('Stop Housekeeper thread')
        self.__is_alive = False
        self.join(JOIN_TIMEOUT)