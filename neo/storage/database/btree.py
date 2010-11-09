#
# Copyright (C) 2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
"""
Naive b-tree implementation.
Simple, though not so well tested.
Not persistent ! (no data retained after process exit)
"""

from BTrees.OOBTree import OOBTree as _OOBTree
import neo

from neo.storage.database import DatabaseManager
from neo.protocol import CellStates
from neo import util

# The only purpose of this value (and code using it) is to avoid creating
# arbitrarily-long lists of values when cleaning up dictionaries.
KEY_BATCH_SIZE = 1000

# Keep dropped trees in memory to avoid instanciating when not needed.
TREE_POOL = []
# How many empty BTree istance to keep in ram
MAX_TREE_POOL_SIZE = 100

def OOBTree():
    try:
        result = TREE_POOL.pop()
    except IndexError:
        result = _OOBTree()
    return result

def prune(tree):
    if len(TREE_POOL) < MAX_TREE_POOL_SIZE:
        tree.clear()
        TREE_POOL.append(tree)

class CreationUndone(Exception):
    pass

def iterObjSerials(obj):
    for tserial in obj.values():
        for serial in tserial.keys():
            yield serial

def descItems(tree):
    try:
        key = tree.maxKey()
    except ValueError:
        pass
    else:
        while True:
            yield (key, tree[key])
            try:
                key = tree.maxKey(key - 1)
            except ValueError:
                break

def descKeys(tree):
    try:
        key = tree.maxKey()
    except ValueError:
        pass
    else:
        while True:
            yield key
            try:
                key = tree.maxKey(key - 1)
            except ValueError:
                break

def safeIter(func, *args, **kw):
    try:
        some_list = func(*args, **kw)
    except ValueError:
        some_list = []
    return some_list

class BTreeDatabaseManager(DatabaseManager):

    _obj = None
    _trans = None
    _tobj = None
    _ttrans = None
    _pt = None
    _config = None

    def __init__(self, database):
        super(BTreeDatabaseManager, self).__init__()
        self.setup(reset=1)

    def setup(self, reset=0):
        if reset:
            self._obj = OOBTree()
            self._trans = OOBTree()
            self.dropUnfinishedData()
            self._pt = {}
            self._config = {}

    def _begin(self):
        pass

    def _commit(self):
        pass

    def _rollback(self):
        pass

    def getConfiguration(self, key):
        return self._config[key]

    def _setConfiguration(self, key, value):
        self._config[key] = value

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            result = int(self.getConfiguration('_pack_tid'))
        except KeyError:
            result = -1
        return result

    def getPartitionTable(self):
        pt = []
        append = pt.append
        for (offset, uuid), state in self._pt.iteritems():
            append((offset, uuid, state))
        return pt

    def getLastTID(self, all=True):
        try:
            ltid = self._trans.maxKey()
        except ValueError:
            ltid = None
        if all:
            try:
                tmp_ltid = self._ttrans.maxKey()
            except ValueError:
                tmp_ltid = None
            tmp_serial = None
            for tserial in self._tobj.values():
                try:
                    max_tmp_serial = tserial.maxKey()
                except ValueError:
                    pass
                else:
                    tmp_serial = max(tmp_serial, max_tmp_serial)
            ltid = max(ltid, tmp_ltid, tmp_serial)
        if ltid is not None:
            ltid = util.p64(ltid)
        return ltid

    def getUnfinishedTIDList(self):
        p64 = util.p64
        tid_set = set(p64(x) for x in self._ttrans.keys())
        tid_set.update(p64(x) for x in iterObjSerials(self._tobj))
        return list(tid_set)

    def objectPresent(self, oid, tid, all=True):
        u64 = util.u64
        oid = u64(oid)
        tid = u64(tid)
        try:
            result = self._obj[oid].has_key(tid)
        except KeyError:
            if all:
                try:
                    result = self._tobj[oid].has_key(tid)
                except KeyError:
                    result = False
            else:
                result = False
        return result

    def _getObjectData(self, oid, value_serial, tid):
        if value_serial is None:
            raise CreationUndone
        if value_serial >= tid:
            raise ValueError, "Incorrect value reference found for " \
                "oid %d at tid %d: reference = %d" % (oid, value_serial, tid)
        try:
            tserial = self._obj[oid]
        except KeyError:
            raise IndexError(oid)
        try:
            compression, checksum, value, next_value_serial = tserial[
                value_serial]
        except KeyError:
            raise IndexError(value_serial)
        if value is None:
            neo.logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            value_serial, compression, checksum, value = self._getObjectData(
                oid, next_value_serial, value_serial)
        return value_serial, compression, checksum, value

    def _getObject(self, oid, tid=None, before_tid=None):
        tserial = self._obj.get(oid)
        if tserial is None:
            result = None
        else:
            if tid is None:
                if before_tid is None:
                    try:
                        tid = tserial.maxKey()
                    except ValueError:
                        tid = None
                else:
                    before_tid -= 1
                    try:
                        tid = tserial.maxKey(before_tid)
                    except ValueError:
                        tid = None
            result = tserial.get(tid, None)
            if result:
                compression, checksum, data, value_serial = result
                if before_tid is None:
                    next_serial = None
                else:
                    try:
                        next_serial = tserial.minKey(tid + 1)
                    except ValueError:
                        next_serial = None
                result = (tid, next_serial, compression, checksum, data,
                    value_serial)
        return result

    def doSetPartitionTable(self, ptid, cell_list, reset):
        pt = self._pt
        if reset:
            pt.clear()
        for offset, uuid, state in cell_list:
            # TODO: this logic should move out of database manager
            # add 'dropCells(cell_list)' to API and use one query
            key = (offset, uuid)
            if state == CellStates.DISCARDED:
                pt.pop(key, None)
            else:
                pt[key] = int(state)
        self.setPTID(ptid)

    def changePartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, False)

    def setPartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, True)

    def _dropPartitions(self, num_partitions, offset_list, tree):
        offset_list = frozenset(offset_list)
        last = 0
        while True:
            to_drop = []
            append = to_drop.append
            for key in tree.keys(min=last):
                if key % num_partitions in offset_list:
                    append(key)
                    if len(to_drop) >= KEY_BATCH_SIZE:
                        last = key + 1
                        break
            if to_drop:
                for key in to_drop:
                    prune(tree[key])
                    del tree[key]
            else:
                break

    def dropPartitions(self, num_partitions, offset_list):
        self._dropPartitions(num_partitions, offset_list, self._obj)
        self._dropPartitions(num_partitions, offset_list, self._trans)

    def dropUnfinishedData(self):
        self._tobj = OOBTree()
        self._ttrans = OOBTree()

    def storeTransaction(self, tid, object_list, transaction, temporary=True):
        u64 = util.u64
        tid = u64(tid)
        if temporary:
            obj = self._tobj
            trans = self._ttrans
        else:
            obj = self._obj
            trans = self._trans
        for oid, compression, checksum, data, value_serial in object_list:
            oid = u64(oid)
            if data is None:
                compression = checksum = data
            else:
                # TODO: unit-test this raise
                if value_serial is not None:
                    raise ValueError, 'Either data or value_serial ' \
                        'must be None (oid %d, tid %d)' % (oid, tid)
            try:
                tserial = obj[oid]
            except KeyError:
                tserial = obj[oid] = OOBTree()
            if value_serial is not None:
                value_serial = u64(value_serial)
            tserial[tid] = (compression, checksum, data, value_serial)

        if transaction is not None:
            oid_list, user, desc, ext, packed = transaction
            trans[tid] = (tuple(oid_list), user, desc, ext, packed)

    def _getDataTIDFromData(self, oid, result):
        tid, _, _, _, data, value_serial = result
        if data is None:
            try:
                data_serial = self._getObjectData(oid, value_serial, tid)[0]
            except CreationUndone:
                data_serial = None
        else:
            data_serial = tid
        return tid, data_serial

    def _getDataTID(self, oid, tid=None, before_tid=None):
        result = self._getObject(oid, tid=tid, before_tid=before_tid)
        if result is None:
            result = (None, None)
        else:
            result = self._getDataTIDFromData(oid, result)
        return result

    def finishTransaction(self, tid):
        tid = util.u64(tid)
        obj = self._obj
        tobj = self._tobj
        ttrans = self._ttrans
        def callback(oid, data):
            try:
                tserial = obj[oid]
            except KeyError:
                tserial = obj[oid] = OOBTree()
            tserial[tid] = data
        self._popTransactionFromObj(tobj, tid, callback=callback)
        try:
            data = ttrans[tid]
        except KeyError:
            pass
        else:
            del ttrans[tid]
            self._trans[tid] = data

    def _popTransactionFromObj(self, tree, tid, callback=None):
        if callback is None:
            callback = lambda oid, data: None
        last = 0
        while True:
            to_remove = []
            append = to_remove.append
            for oid, tserial in tree.items(min=last):
                try:
                    data = tserial[tid]
                except KeyError:
                    continue
                del tserial[tid]
                if not tserial:
                    append(oid)
                callback(oid, data)
                if len(to_remove) >= KEY_BATCH_SIZE:
                    last = oid + 1
                    break
            if to_remove:
                for oid in to_remove:
                    prune(tree[oid])
                    del tree[oid]
            else:
                break

    def deleteTransaction(self, tid, all=False):
        tid = util.u64(tid)
        self._popTransactionFromObj(self._tobj, tid)
        try:
            del self._ttrans[tid]
        except KeyError:
            pass
        if all:
            self._popTransactionFromObj(self._obj, tid)
            try:
                del self._trans[tid]
            except KeyError:
                pass

    def deleteObject(self, oid, serial=None):
        u64 = util.u64
        oid = u64(oid)
        obj = self._obj
        try:
            tserial = obj[oid]
        except KeyError:
            pass
        else:
            if serial is None:
                del obj[oid]
            else:
                serial = u64(serial)
                try:
                    del tserial[serial]
                except KeyError:
                    pass

    def getTransaction(self, tid, all=False):
        tid = util.u64(tid)
        try:
            result = self._trans[tid]
        except KeyError:
            if all:
                try:
                    result = self._ttrans[tid]
                except KeyError:
                    result = None
            else:
                result = None
        if result is not None:
            oid_list, user, desc, ext, packed = result
            result = (list(oid_list), user, desc, ext, packed)
        return result

    def getOIDList(self, min_oid, length, num_partitions,
            partition_list):
        p64 = util.p64
        partition_list = frozenset(partition_list)
        result = []
        append = result.append
        for oid in safeIter(self._obj.keys, min=min_oid):
            if oid % num_partitions in partition_list:
                if length == 0:
                    break
                length -= 1
                append(p64(oid))
        return result

    def _getObjectLength(self, oid, value_serial):
        if value_serial is None:
            raise CreationUndone
        _, _, value, value_serial = self._obj[oid][value_serial]
        if value is None:
            neo.logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            length = self._getObjectLength(oid, value_serial)
        else:
            length = len(value)
        return length

    def getObjectHistory(self, oid, offset=0, length=1):
        # FIXME: This method doesn't take client's current ransaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        oid = util.u64(oid)
        p64 = util.p64
        pack_tid = self._getPackTID()
        try:
            tserial = self._obj[oid]
        except KeyError:
            result = None
        else:
            result = []
            append = result.append
            tserial_iter = descItems(tserial)
            while offset > 0:
                tserial_iter.next()
                offset -= 1
            for serial, (_, _, value, value_serial) in tserial_iter:
                if length == 0 or serial < pack_tid:
                    break
                length -= 1
                if value is None:
                    try:
                        data_length = self._getObjectLength(oid, value_serial)
                    except CreationUndone:
                        data_length = 0
                else:
                    data_length = len(value)
                append((p64(serial), data_length))
        if not result:
            result = None
        return result

    def getObjectHistoryFrom(self, min_oid, min_serial, max_serial, length,
            num_partitions, partition):
        u64 = util.u64
        p64 = util.p64
        min_oid = u64(min_oid)
        min_serial = u64(min_serial)
        max_serial = u64(max_serial)
        result = {}
        for oid, tserial in safeIter(self._obj.items, min=min_oid):
            if oid % num_partitions == partition:
                result[p64(oid)] = tid_list = []
                append = tid_list.append
                if oid == min_oid:
                    try:
                        tid_seq = tserial.keys(min=min_serial,  max=max_serial)
                    except ValueError:
                        continue
                else:
                    tid_seq = tserial.keys(max=max_serial)
                for tid in tid_seq:
                    if length == 0:
                        break
                    length -= 1
                    append(p64(tid))
                else:
                    continue
                break
        return result

    def getTIDList(self, offset, length, num_partitions, partition_list):
        p64 = util.p64
        partition_list = frozenset(partition_list)
        result = []
        append = result.append
        trans_iter = descKeys(self._trans)
        while offset > 0:
            tid = trans_iter.next()
            if tid % num_partitions in partition_list:
                offset -= 1
        for tid in trans_iter:
            if tid % num_partitions in partition_list:
                if length == 0:
                    break
                length -= 1
                append(p64(tid))
        return result

    def getReplicationTIDList(self, min_tid, max_tid, length, num_partitions,
            partition):
        p64 = util.p64
        u64 = util.u64
        result = []
        append = result.append
        for tid in safeIter(self._trans.keys, min=u64(min_tid), max=u64(max_tid)):
            if tid % num_partitions == partition:
                if length == 0:
                    break
                length -= 1
                append(p64(tid))
        return result

    def _updatePackFuture(self, oid, orig_serial, max_serial,
            updateObjectDataForPack):
        p64 = util.p64
        # Before deleting this objects revision, see if there is any
        # transaction referencing its value at max_serial or above.
        # If there is, copy value to the first future transaction. Any further
        # reference is just updated to point to the new data location.
        value_serial = None
        obj = self._obj
        for tree in (obj, self._tobj):
            try:
                tserial = tree[oid]
            except KeyError:
                continue
            for serial, record in tserial.items(
                    min=max_serial):
                if record[3] == orig_serial:
                    if value_serial is None:
                        value_serial = serial
                        tserial[serial] = tserial[orig_serial]
                    else:
                        record = list(record)
                        record[3] = value_serial
                        tserial[serial] = tuple(record)
        def getObjectData():
            assert value_serial is None
            return obj[oid][orig_serial][:3]
        if value_serial:
            value_serial = p64(value_serial)
        updateObjectDataForPack(p64(oid), p64(orig_serial), value_serial,
            getObjectData)

    def pack(self, tid, updateObjectDataForPack):
        tid = util.u64(tid)
        updatePackFuture = self._updatePackFuture
        self._setPackTID(tid)
        obj = self._obj
        last_obj = 0
        while True:
            obj_to_drop = []
            append_obj = obj_to_drop.append
            for oid, tserial in safeIter(obj.items, min=last_obj):
                try:
                    max_serial = tserial.maxKey(tid)
                except ValueError:
                    continue
                try:
                    tserial.maxKey(max_serial)
                except ValueError:
                    if tserial[max_serial][2] == '':
                        max_serial += 1
                    else:
                        continue
                last = 0
                while True:
                    to_drop = []
                    append = to_drop.append
                    for serial in tserial.keys(min=last, max=max_serial,
                            excludemax=True):
                        updatePackFuture(oid, serial, max_serial,
                            updateObjectDataForPack)
                        append(serial)
                        if len(to_drop) >= KEY_BATCH_SIZE:
                            last = serial + 1
                            break
                    if to_drop:
                        for serial in to_drop:
                            del tserial[serial]
                    else:
                        break
                if not tserial:
                    append_obj(oid)
                    if len(obj_to_drop) >= KEY_BATCH_SIZE:
                        last_obj = oid + 1
                        break
            if obj_to_drop:
                for oid in to_drop:
                    prune(obj[oid])
                    del obj[oid]
            else:
                break

    def checkTIDRange(self, min_tid, length, num_partitions, partition):
        # XXX: XOR is a lame checksum
        count = 0
        tid_checksum = 0
        max_tid = 0
        for max_tid in safeIter(self._trans.keys, min=util.u64(min_tid)):
            if max_tid % num_partitions == partition:
                if count >= length:
                    break
                tid_checksum ^= max_tid
                count += 1
        return count, tid_checksum, util.p64(max_tid)

    def checkSerialRange(self, min_oid, min_serial, length, num_partitions,
            partition):
        # XXX: XOR is a lame checksum
        u64 = util.u64
        p64 = util.p64
        min_oid = u64(min_oid)
        count = 0
        oid_checksum = serial_checksum = 0
        max_oid = max_serial = 0
        for max_oid, tserial in safeIter(self._obj.items, min=min_oid):
            if max_oid % num_partitions == partition:
                if max_oid == min_oid:
                    try:
                        serial_iter = tserial.keys(min=u64(min_serial))
                    except ValueError:
                        continue
                else:
                    serial_iter = tserial.keys()
                for max_serial in serial_iter:
                    if count >= length:
                        break
                    oid_checksum ^= max_oid
                    serial_checksum ^= max_serial
                    count += 1
                if count >= length:
                    break
        return count, oid_checksum, p64(max_oid), serial_checksum, p64(max_serial)

