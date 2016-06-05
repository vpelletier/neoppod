import os
import sys
import threading
import traceback
from collections import deque
from time import time
from Queue import Empty

"""
  Verbose locking classes.

  Python threading module contains a simple logging mechanism, but:
    - It's limitted to RLock class
    - It's enabled instance by instance
    - Choice to log or not is done at instanciation
    - It does not emit any log before trying to acquire lock

  This file defines a VerboseLock class implementing basic lock API and
  logging in appropriate places with extensive details.

  It can be globaly toggled by setting NEO_VERBOSE_LOCKING environment variable
  to a non-empty value before this module is loaded.
  There is no overhead at all when disabled (passthrough to threading
  classes).
"""

VERBOSE_LOCKING = bool(os.getenv('NEO_VERBOSE_LOCKING'))


class LockUser(object):

    def __init__(self, message, level=0):
        t = threading.currentThread()
        ident = getattr(t, 'node_name', t.name)
        # This class is instanciated from a place desiring to known what
        # called it.
        # limit=1 would return execution position in this method
        # limit=2 would return execution position in caller
        # limit=3 returns execution position in caller's caller
        # Additionnal level value (should be positive only) can be used when
        # more intermediate calls are involved
        self.stack = stack = traceback.extract_stack()[:-2-level]
        path, line_number, func_name, line = stack[-1]
        # Simplify path. Only keep 3 last path elements. It is enough for
        # current Neo directory structure.
        path = os.path.join('...', *path.split(os.path.sep)[-3:])
        self.time = time()
        self.ident = "%s@%r %s:%s %s" % (
            ident, self.time, path, line_number, line)
        self.note(message)
        self.ident = ident

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.ident == other.ident

    def __repr__(self):
        return "%s@%r" % (self.ident, self.time)

    def formatStack(self):
        return ''.join(traceback.format_list(self.stack))

    def note():
        write = sys.stderr.write
        flush = sys.stderr.flush
        def note(self, message):
            write("[%s] %s\n" % (self.ident, message))
            flush()
        return note
    note = note()


class VerboseLockBase(object):

    _error_class = threading.ThreadError
    _release_error = 'release unlocked lock'

    def __init__(self, check_owner, name=None, verbose=None):
        self._check_owner = check_owner
        self._name = name or '<%s@%X>' % (self.__class__.__name__, id(self))
        self.owner = None
        self.waiting = []
        LockUser(repr(self) + " created", 1)

    def acquire(self, blocking=1):
        owner = self.owner if self._locked() else None
        me = LockUser("%s.acquire(%s). Owned by %r. Waiting: %r"
                      % (self, blocking, owner, self.waiting))
        if blocking:
            if self._check_owner and me == owner:
                me.note("I already own this lock: %r" % owner)
                me.note("Owner traceback:\n%s" % owner.formatStack())
                me.note("My traceback:\n%s" % me.formatStack())
            self.waiting.append(me)
        try:
            locked = self.lock.acquire(blocking)
        finally:
            if blocking:
                self.waiting.remove(me)
        if locked:
            self.owner = me
            me.note("Lock granted. Waiting: " + repr(self.waiting))
        return locked

    __enter__ = acquire

    def release(self):
        me = LockUser("%s.release(). Waiting: %r" % (self, self.waiting))
        try:
            return self.lock.release()
        except self._error_class:
            t, v, tb = sys.exc_info()
            if str(v) == self._release_error:
                raise t, "%s %s (%s)" % (v, self, me), tb
            raise

    def __exit__(self, t, v, tb):
        self.release()

    def _locked(self):
        raise NotImplementedError

    def __repr__(self):
        return self._name


class VerboseRLock(VerboseLockBase):

    _error_class = RuntimeError
    _release_error = 'cannot release un-acquired lock'

    def __init__(self, **kw):
        super(VerboseRLock, self).__init__(check_owner=False, **kw)
        self.lock = threading.RLock()

    def _locked(self):
        return self.lock._RLock__block.locked()

    def _is_owned(self):
        return self.lock._is_owned()

class VerboseLock(VerboseLockBase):

    def __init__(self, check_owner=True, **kw):
        super(VerboseLock, self).__init__(check_owner, **kw)
        self.lock = threading.Lock()

    def locked(self):
        return self.lock.locked()
    _locked = locked

class VerboseSemaphore(VerboseLockBase):

    def __init__(self, value=1, check_owner=True, **kw):
        super(VerboseSemaphore, self).__init__(check_owner, **kw)
        self.lock = threading.Semaphore(value)

    def _locked(self):
        return not self.lock._Semaphore__value

# XXX: threading.Event is a function instanciating threading._Event.. WTF ?
threading_Event = threading.Event().__class__

class VerboseEvent(threading_Event):

    def __init__(self, *args, **kw):
        self.waiting = []
        self.creator = LockUser(repr(self) + " created")
        super(VerboseEvent, self).__init__(*args, **kw)

    def wait(self, timeout=None):
        me = LockUser("%r.wait(%s). Creator: %r. Waiting: %r"
                      % (self, timeout, self.creator, self.waiting))
        self.waiting.append(me)
        try:
            return super(VerboseEvent, self).wait(timeout)
        finally:
            self.waiting.remove(me)


if VERBOSE_LOCKING:
    Lock = VerboseLock
    RLock = VerboseRLock
    Semaphore = VerboseSemaphore
    Event = VerboseEvent
else:
    Lock = threading.Lock
    RLock = threading.RLock
    Semaphore = threading.Semaphore
    Event = threading.Event


class SimpleQueue(object):
    """
    Similar to Queue.Queue but with simpler locking scheme, reducing lock
    contention on "put" (benchmark shows 60% less time spent in "put").
    As a result:
    - only a single consumer possible ("get" vs. "get" race condition)
    - only a single producer possible ("put" vs. "put" race condition)
    - no blocking size limit possible
    - no consumer -> producer notifications (task_done/join API)

    Queue is on the critical path: any moment spent here increases client
    application wait for object data, transaction completion, etc.
    As we have a single consumer (client application's thread) and a single
    producer (lib.dispatcher, which can be called from several threads but
    serialises calls internally) for each queue, Queue.Queue's locking scheme
    can be relaxed to reduce latency.
    """
    __slots__ = ('_lock', '_unlock', '_popleft', '_append', '_queue')
    def __init__(self):
        lock = Lock()
        self._lock = lock.acquire
        self._unlock = lock.release
        self._queue = queue = deque()
        self._popleft = queue.popleft
        self._append = queue.append

    def get(self, block):
        if block:
            self._lock(False)
        while True:
            try:
                return self._popleft()
            except IndexError:
                if not block:
                    raise Empty
                self._lock()

    def put(self, item):
        self._append(item)
        self._lock(False)
        self._unlock()

    def empty(self):
        return not self._queue

class _SharedLockBaseLock(object):
    def __init__(self, condition, counter):
        self._condition = condition
        self._counter = counter

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    @staticmethod
    def acquire(*args, **kw):
        raise NotImplementedError

    @staticmethod
    def release():
        raise NotImplementedError

class _SharedLockCounter(object):
    def __init__(self):
        self._value = 0

    def inc(self):
        self._value += 1

    def dec(self):
        assert self._value
        self._value -= 1
        return not self._value

    def __nonzero__(self):
        return bool(self._value)

class _SharedLockWriter(_SharedLockBaseLock):
    def acquire(self, timeout=None):
        self._condition.acquire()
        while self._counter:
            self._condition.wait(timeout=timeout)

    def release(self):
        self._condition.release()

class _SharedLockReader(_SharedLockBaseLock):
    def acquire(self):
        with self._condition:
            self._counter.inc()

    def release(self):
        with self._condition:
            if self._counter.dec():
                self._condition.notify()

class SharedLock(object):
    def __init__(self, lock=None):
        condition = Condition(RLock() if lock is None else lock)
        reader_count = _SharedLockCounter()
        self.read = _SharedLockReader(condition, reader_count)
        self.write = _SharedLockWriter(condition, reader_count)
