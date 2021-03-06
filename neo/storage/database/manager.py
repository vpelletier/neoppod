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

from collections import defaultdict
from functools import wraps
from neo.lib import logging, util
from neo.lib.exception import DatabaseFailure
from neo.lib.interfaces import abstract, requires
from neo.lib.protocol import ZERO_TID

def lazymethod(func):
    def getter(self):
        cls = self.__class__
        name = func.__name__
        assert name not in cls.__dict__
        setattr(cls, name, func(self))
        return getattr(self, name)
    return property(getter, doc=func.__doc__)

def fallback(func):
    def warn(self):
        logging.info("Fallback to generic/slow implementation of %s."
            " It should be overridden by backend storage (%s).",
            func.__name__, self.__class__.__name__)
        return func
    return lazymethod(wraps(func)(warn))

def splitOIDField(tid, oids):
    if len(oids) % 8:
        raise DatabaseFailure('invalid oids length for tid %s: %s'
            % (tid, len(oids)))
    return [oids[i:i+8] for i in xrange(0, len(oids), 8)]

class CreationUndone(Exception):
    pass

class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    ENGINES = ()

    def __init__(self, database, engine=None, wait=0):
        """
            Initialize the object.
        """
        if engine:
            if engine not in self.ENGINES:
                raise ValueError("Unsupported engine: %r not in %r"
                                 % (engine, self.ENGINES))
            self._engine = engine
        self._wait = wait
        self._deferred = 0
        self._parse(database)

    def __getattr__(self, attr):
        if attr == "_getPartition":
            np = self.getNumPartitions()
            value = lambda x: x % np
        else:
            return self.__getattribute__(attr)
        setattr(self, attr, value)
        return value

    @abstract
    def _parse(self, database):
        """Called during instanciation, to process database parameter."""

    def setup(self, reset=0):
        """Set up a database, discarding existing data first if reset is True
        """
        if reset:
            self.erase()
        self._uncommitted_data = defaultdict(int)
        self._setup()

    @abstract
    def erase(self):
        """"""

    @abstract
    def _setup(self):
        """To be overriden by the backend to set up a database

        It must recover self._uncommitted_data from temporary object table.
        _uncommitted_data is already instantiated and must be updated with
        refcounts to data of write-locked objects, except in case of undo,
        where the refcount is increased later, when the object is read-locked.
        Keys are data ids and values are number of references.
        """

    @abstract
    def nonempty(self, table):
        """Check whether table is empty or return None if it does not exist"""

    def _checkNoUnfinishedTransactions(self, *hint):
        if self.nonempty('ttrans') or self.nonempty('tobj'):
            raise DatabaseFailure(
                "The database can not be upgraded because you have unfinished"
                " transactions. Use an older version of NEO to verify them.")

    def _getVersion(self):
        version = int(self.getConfiguration("version") or 0)
        if self.VERSION < version:
            raise DatabaseFailure("The database can not be downgraded.")
        return version

    def doOperation(self, app):
        pass

    def _close(self):
        """Backend-specific code to close the database"""

    @requires(_close)
    def close(self):
        self._deferredCommit()
        self._close()

    def _commit(self):
        """Backend-specific code to commit the pending changes"""

    @requires(_commit)
    def commit(self):
        logging.debug('committing...')
        self._commit()
        # Instead of cancelling a timeout that would be set to defer a commit,
        # we simply use to a boolean so that _deferredCommit() does nothing.
        # IOW, epoll may wait wake up for nothing but that should be rare,
        # because most immediate commits are usually quickly followed by
        # deferred commits.
        self._deferred = 0

    def deferCommit(self):
        self._deferred = 1
        return self._deferredCommit

    def _deferredCommit(self):
        if self._deferred:
            self.commit()

    @abstract
    def getConfiguration(self, key):
        """
            Return a configuration value, returns None if not found or not set
        """

    def setConfiguration(self, key, value):
        """
            Set a configuration value
        """
        self._setConfiguration(key, value)
        self.commit()

    @abstract
    def _setConfiguration(self, key, value):
        """"""

    def getUUID(self):
        """
            Load a NID from a database.
        """
        nid = self.getConfiguration('nid')
        if nid is not None:
            return int(nid)

    def setUUID(self, nid):
        """
            Store a NID into a database.
        """
        self.setConfiguration('nid', str(nid))

    def getNumPartitions(self):
        """
            Load the number of partitions from a database.
        """
        n = self.getConfiguration('partitions')
        if n is not None:
            return int(n)

    def setNumPartitions(self, num_partitions):
        """
            Store the number of partitions into a database.
        """
        self.setConfiguration('partitions', num_partitions)
        try:
            del self._getPartition
        except AttributeError:
            pass

    def getNumReplicas(self):
        """
            Load the number of replicas from a database.
        """
        n = self.getConfiguration('replicas')
        if n is not None:
            return int(n)

    def setNumReplicas(self, num_replicas):
        """
            Store the number of replicas into a database.
        """
        self.setConfiguration('replicas', num_replicas)

    def getName(self):
        """
            Load a name from a database.
        """
        return self.getConfiguration('name')

    def setName(self, name):
        """
            Store a name into a database.
        """
        self.setConfiguration('name', name)

    def getPTID(self):
        """
            Load a Partition Table ID from a database.
        """
        ptid = self.getConfiguration('ptid')
        if ptid is not None:
            return int(ptid)

    def setPTID(self, ptid):
        """
            Store a Partition Table ID into a database.
        """
        if ptid is not None:
            assert isinstance(ptid, (int, long)), ptid
            ptid = str(ptid)
        self.setConfiguration('ptid', ptid)

    def getBackupTID(self):
        return util.bin(self.getConfiguration('backup_tid'))

    def _setBackupTID(self, tid):
        tid = util.dump(tid)
        logging.debug('backup_tid = %s', tid)
        return self._setConfiguration('backup_tid', tid)

    def getTruncateTID(self):
        return util.bin(self.getConfiguration('truncate_tid'))

    def _setTruncateTID(self, tid):
        tid = util.dump(tid)
        logging.debug('truncate_tid = %s', tid)
        return self._setConfiguration('truncate_tid', tid)

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            return int(self.getConfiguration('_pack_tid'))
        except TypeError:
            return -1

    @abstract
    def getPartitionTable(self):
        """Return a whole partition table as a sequence of rows. Each row
        is again a tuple of an offset (row ID), the NID of a storage
        node, and a cell state."""

    @abstract
    def getLastTID(self, max_tid):
        """Return greatest tid in trans table that is <= given 'max_tid'

        Required only to import a DB using Importer backend.
        max_tid must be in unpacked format.
        """

    def _getLastIDs(self):
        """"""

    @requires(_getLastIDs)
    def getLastIDs(self):
        trans, obj, oid = self._getLastIDs()
        if trans:
            tid = max(trans.itervalues())
            if obj:
                tid = max(tid, max(obj.itervalues()))
        else:
            tid = max(obj.itervalues()) if obj else None
        # TODO: Replication can't be resumed from the tids in 'trans' and 'obj'
        #       because outdated cells are writable and may contain recently
        #       committed data. We must save somewhere where replication was
        #       interrupted and return this information. For the moment, we
        #       tell the replicator to resume from the beginning.
        trans = obj = {}
        return tid, trans, obj, oid

    def _getUnfinishedTIDDict(self):
        """"""

    @requires(_getUnfinishedTIDDict)
    def getUnfinishedTIDDict(self):
        trans, obj = self._getUnfinishedTIDDict()
        obj = dict.fromkeys(obj)
        obj.update(trans)
        p64 = util.p64
        return {p64(ttid): None if tid is None else p64(tid)
                for ttid, tid in obj.iteritems()}

    @fallback
    def getLastObjectTID(self, oid):
        """Return the latest tid of given oid or None if it does not exist"""
        r = self.getObject(oid)
        return r and r[0]

    @abstract
    def _getNextTID(self, partition, oid, tid):
        """
        partition (int)
            Must be the result of (oid % self.getPartition(oid))
        oid (int)
            Identifier of object to retrieve.
        tid (int)
            Exact serial to retrieve.

        If tid is the last revision of oid, None is returned.
        """

    def _getObject(self, oid, tid=None, before_tid=None):
        """
        oid (int)
            Identifier of object to retrieve.
        tid (int, None)
            Exact serial to retrieve.
        before_tid (packed, None)
            Serial to retrieve is the highest existing one strictly below this
            value.
        """

    @requires(_getObject)
    def getObject(self, oid, tid=None, before_tid=None):
        """
        oid (packed)
            Identifier of object to retrieve.
        tid (packed, None)
            Exact serial to retrieve.
        before_tid (packed, None)
            Serial to retrieve is the highest existing one strictly below this
            value.

        Return value:
            None: Given oid doesn't exist in database.
            False: No record found, but another one exists for given oid.
            6-tuple: Record content.
                - record serial (packed)
                - serial or next record modifying object (packed, None)
                - compression (boolean-ish, None)
                - checksum (integer, None)
                - data (binary string, None)
                - data_serial (packed, None)
        """
        u64 = util.u64
        r = self._getObject(u64(oid), tid and u64(tid),
                            before_tid and u64(before_tid))
        try:
            serial, next_serial, compression, checksum, data, data_serial = r
        except TypeError:
            # See if object exists at all
            return (tid or before_tid) and self.getLastObjectTID(oid) and False
        return (util.p64(serial),
                None if next_serial is None else util.p64(next_serial),
                compression, checksum, data,
                None if data_serial is None else util.p64(data_serial))

    @abstract
    def changePartitionTable(self, ptid, cell_list, reset=False):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        the NID of a storage node, and a cell state. The Partition
        Table ID must be stored as well. If reset is True, existing data
        is first thrown away."""

    @abstract
    def dropPartitions(self, offset_list):
        """Delete all data for specified partitions"""

    @abstract
    def dropUnfinishedData(self):
        """Drop any unfinished data from a database."""

    @abstract
    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        """Write transaction metadata

        The list of objects contains tuples, each of which consists of
        an object ID, a data_id and object serial.
        The transaction is either None or a tuple of the list of OIDs,
        user information, a description, extension information and transaction
        pack state (True for packed).

        If 'temporary', the transaction is stored into ttrans/tobj tables,
        (instead of trans/obj). The caller is in charge of committing, which
        is always the case at tpc_vote.
        """

    @abstract
    def _pruneData(self, data_id_list):
        """To be overriden by the backend to delete any unreferenced data

        'unreferenced' means:
        - not in self._uncommitted_data
        - and not referenced by a fully-committed object (storage should have
          an index or a refcount of all data ids of all objects)
        """

    @abstract
    def storeData(self, checksum, data, compression):
        """To be overriden by the backend to store object raw data

        If same data was already stored, the storage only has to check there's
        no hash collision.
        """

    def holdData(self, checksum_or_id, *args):
        """Store raw data of temporary object

        If 'checksum_or_id' is a checksum, it must be the result of
        makeChecksum(data) and extra parameters must be (data, compression)
        where 'compression' indicates if 'data' is compressed.
        A volatile reference is set to this data until 'releaseData' is called
        with this checksum.
        If called with only an id, it only increment the volatile
        reference to the data matching the id.
        """
        if args:
            checksum_or_id = self.storeData(checksum_or_id, *args)
        self._uncommitted_data[checksum_or_id] += 1
        return checksum_or_id

    def releaseData(self, data_id_list, prune=False):
        """Release 1 volatile reference to given list of data ids

        If 'prune' is true, any data that is not referenced anymore (either by
        a volatile reference or by a fully-committed object) is deleted.
        """
        refcount = self._uncommitted_data
        for data_id in data_id_list:
            count = refcount[data_id] - 1
            if count:
                refcount[data_id] = count
            else:
                del refcount[data_id]
        if prune:
            self._pruneData(data_id_list)
            self.commit()

    @fallback
    def _getDataTID(self, oid, tid=None, before_tid=None):
        """
        Return a 2-tuple:
        tid (int)
            tid corresponding to received parameters
        serial
            data tid of the found record

        (None, None) is returned if requested object and transaction
        could not be found.

        This method only exists for performance reasons, by not returning data:
        _getObject already returns these values but it is slower.
        """
        r = self._getObject(oid, tid, before_tid)
        return (r[0], r[-1]) if r else (None, None)

    def findUndoTID(self, oid, tid, ltid, undone_tid, transaction_object):
        """
        oid
            Object OID
        tid
            Transation doing the undo
        ltid
            Upper (exclued) bound of transactions visible to transaction doing
            the undo.
        undone_tid
            Transaction to undo
        transaction_object
            Object data from memory, if it was modified by running
            transaction.
            None if is was not modified by running transaction.

        Returns a 3-tuple:
        current_tid (p64)
            TID of most recent version of the object client's transaction can
            see. This is used later to detect current conflicts (eg, another
            client modifying the same object in parallel)
        data_tid (int)
            TID containing (without indirection) the data prior to undone
            transaction.
            None if object doesn't exist prior to transaction being undone
              (its creation is being undone).
        is_current (bool)
            False if object was modified by later transaction (ie, data_tid is
            not current), True otherwise.
        """
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        tid = u64(tid)
        if ltid:
            ltid = u64(ltid)
        undone_tid = u64(undone_tid)
        def getDataTID(tid=None, before_tid=None):
            tid, data_tid = self._getDataTID(oid, tid, before_tid)
            current_tid = tid
            while data_tid:
                if data_tid < tid:
                    tid, data_tid = self._getDataTID(oid, data_tid)
                    if tid is not None:
                        continue
                logging.error("Incorrect data serial for oid %s at tid %s",
                              oid, current_tid)
                return current_tid, current_tid
            return current_tid, tid
        if transaction_object:
            current_tid = current_data_tid = u64(transaction_object[2])
        else:
            current_tid, current_data_tid = getDataTID(before_tid=ltid)
        if current_tid is None:
            return (None, None, False)
        found_undone_tid, undone_data_tid = getDataTID(tid=undone_tid)
        assert found_undone_tid is not None, (oid, undone_tid)
        is_current = undone_data_tid in (current_data_tid, tid)
        # Load object data as it was before given transaction.
        # It can be None, in which case it means we are undoing object
        # creation.
        _, data_tid = getDataTID(before_tid=undone_tid)
        if data_tid is not None:
            data_tid = p64(data_tid)
        return p64(current_tid), data_tid, is_current

    @abstract
    def lockTransaction(self, tid, ttid):
        """Mark voted transaction 'ttid' as committed with given 'tid'

        All pending changes are committed just before returning to the caller.
        """

    @abstract
    def unlockTransaction(self, tid, ttid):
        """Finalize a transaction by moving data to a finished area."""

    @abstract
    def abortTransaction(self, ttid):
        """"""

    @abstract
    def deleteTransaction(self, tid):
        """"""

    @abstract
    def deleteObject(self, oid, serial=None):
        """Delete given object. If serial is given, only delete that serial for
        given oid."""

    @abstract
    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        """Delete all objects and transactions between given min_tid (excluded)
        and max_tid (included)"""

    def truncate(self):
        tid = self.getTruncateTID()
        if tid:
            assert tid != ZERO_TID, tid
            for partition in xrange(self.getNumPartitions()):
                self._deleteRange(partition, tid)
            self._setTruncateTID(None)
            self.commit()

    @abstract
    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""

    @abstract
    def getObjectHistory(self, oid, offset, length):
        """Return a list of serials and sizes for a given object ID.
        The length specifies the maximum size of such a list. Result starts
        with latest serial, and the list must be sorted in descending order.
        If there is no such object ID in a database, return None."""

    @abstract
    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        """Return a dict of length oids grouped by serial at (or above)
        min_tid and min_oid and below max_tid, for given partition,
        sorted in ascending order."""

    @abstract
    def getTIDList(self, offset, length, partition_list):
        """Return a list of TIDs in ascending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""

    @abstract
    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        """Return a list of TIDs in ascending order from an initial tid value,
        at most the specified length up to max_tid. The partition number is
        passed to filter out non-applicable TIDs."""

    @abstract
    def pack(self, tid, updateObjectDataForPack):
        """Prune all non-current object revisions at given tid.
        updateObjectDataForPack is a function called for each deleted object
        and revision with:
        - OID
        - packed TID
        - new value_serial
            If object data was moved to an after-pack-tid revision, this
            parameter contains the TID of that revision, allowing to backlink
            to it.
        - getObjectData function
            To call if value_serial is None and an object needs to be updated.
            Takes no parameter, returns a 3-tuple: compression, data_id,
            value
        """

    @abstract
    def checkTIDRange(self, partition, length, min_tid, max_tid):
        """
        Generate a diggest from transaction list.
        min_tid (packed)
            TID at which verification starts.
        length (int)
            Maximum number of records to include in result.

        Returns a 3-tuple:
            - number of records actually found
            - a SHA1 computed from record's TID
              ZERO_HASH if no record found
            - biggest TID found (ie, TID of last record read)
              ZERO_TID if not record found
        """

    @abstract
    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        """
        Generate a diggest from object list.
        min_oid (packed)
            OID at which verification starts.
        min_tid (packed)
            Serial of min_oid object at which search should start.
        length
            Maximum number of records to include in result.

        Returns a 5-tuple:
            - number of records actually found
            - a SHA1 computed from record's OID
              ZERO_HASH if no record found
            - biggest OID found (ie, OID of last record read)
              ZERO_OID if no record found
            - a SHA1 computed from record's serial
              ZERO_HASH if no record found
            - biggest serial found for biggest OID found (ie, serial of last
              record read)
              ZERO_TID if no record found
        """
