#
# Copyright (C) 2006-2016  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from collections import deque
from time import time
from struct import pack, unpack
from neo.lib import logging
from neo.lib.protocol import ProtocolError, uuid_str, ZERO_OID, ZERO_TID
from neo.lib.util import dump, u64, addTID, tidFromTime

class DelayedError(Exception):
    pass

class Transaction(object):
    """
        A pending transaction
    """
    _tid = None
    _msg_id = None
    _oid_list = None
    _prepared = False
    # uuid dict hold flag to known who has locked the transaction
    _uuid_set = None
    _lock_wait_uuid_set = None

    def __init__(self, node, ttid):
        """
            Prepare the transaction, set OIDs and UUIDs related to it
        """
        self._node = node
        self._ttid = ttid
        self._birth = time()
        # store storage uuids that must be notified at commit
        self._notification_set = set()

    def __repr__(self):
        return "<%s(client=%r, tid=%r, oids=%r, storages=%r, age=%.2fs) at %x>" % (
                self.__class__.__name__,
                self._node,
                dump(self._tid),
                map(dump, self._oid_list or ()),
                map(uuid_str, self._uuid_set or ()),
                time() - self._birth,
                id(self),
        )

    def getNode(self):
        """
            Return the node that had began the transaction
        """
        return self._node

    def getTTID(self):
        """
            Return the temporary transaction ID.
        """
        return self._ttid

    def getTID(self):
        """
            Return the transaction ID
        """
        return self._tid

    def getMessageId(self):
        """
            Returns the packet ID to use in the answer
        """
        return self._msg_id

    def getUUIDList(self):
        """
            Returns the list of node's UUID that lock the transaction
        """
        return list(self._uuid_set)

    def getOIDList(self):
        """
            Returns the list of OIDs used in the transaction
        """

        return list(self._oid_list)

    def isPrepared(self):
        """
            Returns True if the commit has been requested by the client
        """
        return self._prepared

    def registerForNotification(self, uuid):
        """
            Register a node that requires a notification at commit
        """
        self._notification_set.add(uuid)

    def getNotificationUUIDList(self):
        """
            Returns the list of nodes waiting for the transaction to be
            finished
        """
        return list(self._notification_set)

    def prepare(self, tid, oid_list, uuid_list, msg_id):

        self._tid = tid
        self._oid_list = oid_list
        self._msg_id = msg_id
        self._uuid_set = set(uuid_list)
        self._lock_wait_uuid_set = set(uuid_list)
        self._prepared = True

    def storageLost(self, uuid):
        """
            Given storage was lost while waiting for its lock, stop waiting
            for it.
            Does nothing if the node was not part of the transaction.
        """
        self._notification_set.discard(uuid)
        # XXX: We might lose information that a storage successfully locked
        # data but was later found to be disconnected. This loss has no impact
        # on current code, but it might be disturbing to reader or future code.
        if self._prepared:
            self._lock_wait_uuid_set.discard(uuid)
            self._uuid_set.discard(uuid)
            return self.locked()
        return False

    def clientLost(self, node):
        if self._node is node:
            if self._prepared:
                self._node = None # orphan
            else:
                return True # abort
        else:
            self._notification_set.discard(node.getUUID())
        return False

    def lock(self, uuid):
        """
            Define that a node has locked the transaction
            Returns true if all nodes are locked
        """
        self._lock_wait_uuid_set.remove(uuid)
        return self.locked()

    def locked(self):
        """
            Returns true if all nodes are locked
        """
        return not self._lock_wait_uuid_set


class TransactionManager(object):
    """
        Manage current transactions
    """

    def __init__(self, on_commit):
        self._on_commit = on_commit
        self.reset()

    def reset(self):
        # ttid -> transaction
        self._ttid_dict = {}
        self._last_oid = ZERO_OID
        self._last_tid = ZERO_TID
        # queue filled with ttids pointing to transactions with increasing tids
        self._queue = deque()

    def __getitem__(self, ttid):
        """
            Return the transaction object for this TID
        """
        try:
            return self._ttid_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))

    def __delitem__(self, ttid):
        try:
            self._queue.remove(ttid)
        except ValueError:
            pass
        del self._ttid_dict[ttid]

    def __contains__(self, ttid):
        """
            Returns True if this is a pending transaction
        """
        return ttid in self._ttid_dict

    def getNextOIDList(self, num_oids):
        """ Generate a new OID list """
        oid = unpack('!Q', self._last_oid)[0] + 1
        oid_list = [pack('!Q', oid + i) for i in xrange(num_oids)]
        self._last_oid = oid_list[-1]
        return oid_list

    def setLastOID(self, oid):
        if self._last_oid < oid:
            self._last_oid = oid

    def getLastOID(self):
        return self._last_oid

    def _nextTID(self, ttid=None, divisor=None):
        """
        Compute the next TID based on the current time and check collisions.
        Also, if ttid is not None, divisor is mandatory adjust it so that
            tid % divisor == ttid % divisor
        while preserving
            min_tid < tid
        If ttid is None, divisor is ignored.
        When constraints allow, prefer decreasing generated TID, to avoid
        fast-forwarding to future dates.
        """
        tid = tidFromTime(time())
        min_tid = self._last_tid
        if tid <= min_tid:
            tid  = addTID(min_tid, 1)
            # We know we won't have room to adjust by decreasing.
            try_decrease = False
        else:
            try_decrease = True
        if ttid is not None:
            assert isinstance(ttid, basestring), repr(ttid)
            assert isinstance(divisor, (int, long)), repr(divisor)
            ref_remainder = u64(ttid) % divisor
            remainder = u64(tid) % divisor
            if ref_remainder != remainder:
                if try_decrease:
                    new_tid = addTID(tid, ref_remainder - divisor - remainder)
                    assert u64(new_tid) % divisor == ref_remainder, (dump(new_tid),
                        ref_remainder)
                    if new_tid <= min_tid:
                        new_tid = addTID(new_tid, divisor)
                else:
                    if ref_remainder > remainder:
                        ref_remainder += divisor
                    new_tid = addTID(tid, ref_remainder - remainder)
                assert min_tid < new_tid, (dump(min_tid), dump(tid), dump(new_tid))
                tid = new_tid
        self._last_tid = tid
        return self._last_tid

    def getLastTID(self):
        """
            Returns the last TID used
        """
        return self._last_tid

    def setLastTID(self, tid):
        """
            Set the last TID, keep the previous if lower
        """
        if self._last_tid < tid:
            self._last_tid = tid

    def hasPending(self):
        """
            Returns True if some transactions are pending
        """
        return bool(self._ttid_dict)

    def registerForNotification(self, uuid):
        """
            Return the list of pending transaction IDs
        """
        # remember that this node must be notified when pending transactions
        # will be finished
        for txn in self._ttid_dict.itervalues():
            txn.registerForNotification(uuid)
        return self._ttid_dict.keys()

    def begin(self, node, tid=None):
        """
            Generate a new TID
        """
        if tid is None:
            # No TID requested, generate a temporary one
            tid = self._nextTID()
        else:
            # Use of specific TID requested, queue it immediately and update
            # last TID.
            self._queue.append(tid)
            self.setLastTID(tid)
        txn = self._ttid_dict[tid] = Transaction(node, tid)
        logging.debug('Begin %s', txn)
        return tid

    def prepare(self, ttid, divisor, oid_list, uuid_list, msg_id):
        """
            Prepare a transaction to be finished
        """
        txn = self[ttid]
        # maybe not the fastest but _queue should be often small
        if ttid in self._queue:
            tid = ttid
        else:
            tid = self._nextTID(ttid, divisor)
            self._queue.append(ttid)
        logging.debug('Finish TXN %s for %s (was %s)',
                      dump(tid), txn.getNode(), dump(ttid))
        txn.prepare(tid, oid_list, uuid_list, msg_id)
        # check if greater and foreign OID was stored
        if oid_list:
            self.setLastOID(max(oid_list))
        return tid

    def abort(self, ttid, uuid):
        """
            Abort a transaction
        """
        logging.debug('Abort TXN %s for %s', dump(ttid), uuid_str(uuid))
        if self[ttid].isPrepared():
            raise ProtocolError("commit already requested for ttid %s"
                                % dump(ttid))
        del self[ttid]

    def lock(self, ttid, uuid):
        """
            Set that a node has locked the transaction.
            If transaction is completely locked, calls function given at
            instanciation time.
        """
        logging.debug('Lock TXN %s for %s', dump(ttid), uuid_str(uuid))
        if self[ttid].lock(uuid) and self._queue[0] == ttid:
            # all storage are locked and we unlock the commit queue
            self._unlockPending()

    def storageLost(self, uuid):
        """
            A storage node has been lost, don't expect a reply from it for
            current transactions
        """
        unlock = False
        for ttid, txn in self._ttid_dict.iteritems():
            if txn.storageLost(uuid) and self._queue[0] == ttid:
                unlock = True
                # do not break: we must call forget() on all transactions
        if unlock:
            self._unlockPending()

    def _unlockPending(self):
        """Serialize transaction unlocks

        This should rarely delay unlocks since the time needed to lock a
        transaction is roughly constant. The most common case where reordering
        is required is when some storages are already busy by other tasks.
        """
        queue = self._queue
        self._on_commit(self._ttid_dict.pop(queue.popleft()))
        while queue:
            ttid = queue[0]
            txn = self._ttid_dict[ttid]
            if not txn.locked():
                break
            del queue[0], self._ttid_dict[ttid]
            self._on_commit(txn)

    def clientLost(self, node):
        for txn in self._ttid_dict.values():
            if txn.clientLost(node):
                del self[txn.getTTID()]

    def log(self):
        logging.info('Transactions:')
        for txn in self._ttid_dict.itervalues():
            logging.info('  %r', txn)

