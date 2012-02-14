#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
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

import random
import sys
import time
import threading
import transaction
import unittest
import neo.lib
from neo.storage.transactions import TransactionManager, \
    DelayedError, ConflictError
from neo.lib.connection import MTClientConnection
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, Packets, \
    ZERO_OID, ZERO_TID, MAX_TID
from . import NEOCluster, NEOThreadedTest, Patch, predictable_random
from neo.client.pool import CELL_CONNECTED, CELL_GOOD


class ReplicationTests(NEOThreadedTest):

    def checksumPartition(self, storage, partition):
        dm = storage.dm
        args = ZERO_TID, MAX_TID, None, partition
        return dm.checkTIDRange(*args), dm.checkSerialRange(ZERO_TID, *args)

    def checkPartitionReplicated(self, source, destination, partition):
        self.assertEqual(self.checksumPartition(source, partition),
                         self.checksumPartition(destination, partition))

    def checkBackup(self, cluster):
        upstream_pt = cluster.upstream.primary_master.pt
        pt = cluster.primary_master.pt
        np = pt.getPartitions()
        self.assertEqual(np, upstream_pt.getPartitions())
        checked = 0
        source_dict = dict((x.uuid, x) for x in cluster.upstream.storage_list)
        for storage in cluster.storage_list:
            self.assertEqual(np, storage.pt.getPartitions())
            for partition in pt.getAssignedPartitionList(storage.uuid):
                cell_list = upstream_pt.getCellList(partition, readable=True)
                source = source_dict[random.choice(cell_list).getUUID()]
                self.checkPartitionReplicated(source, storage, partition)
                checked += 1
        return checked

    def testBackupNormalCase(self):
        upstream = NEOCluster(partitions=7, replicas=1, storage_count=3)
        try:
            upstream.start()
            importZODB = upstream.importZODB()
            importZODB(3)
            upstream.client.setPoll(0)
            backup = NEOCluster(partitions=7, replicas=1, storage_count=5,
                                upstream=upstream)
            try:
                backup.start()
                # Initialize & catch up.
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                backup.tic()
                self.assertEqual(14, self.checkBackup(backup))
                # Normal case, following upstream cluster closely.
                importZODB(17)
                upstream.client.setPoll(0)
                backup.tic()
                self.assertEqual(14, self.checkBackup(backup))
            # Check that a backup cluster can be restarted.
            finally:
                backup.stop()
            backup.reset()
            try:
                backup.start()
                self.assertEqual(backup.neoctl.getClusterState(),
                                 ClusterStates.BACKINGUP)
                importZODB(17)
                upstream.client.setPoll(0)
                backup.tic()
                self.assertEqual(14, self.checkBackup(backup))
                # Stop backing up, nothing truncated.
                backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
                backup.tic()
                self.assertEqual(14, self.checkBackup(backup))
                self.assertEqual(backup.neoctl.getClusterState(),
                                 ClusterStates.RUNNING)
            finally:
                backup.stop()
        finally:
            upstream.stop()

    @predictable_random()
    def testBackupNodeLost(self):
        """Check backup cluster can recover after random connection loss

        - backup master disconnected from upstream master
        - primary storage disconnected from backup master
        - non-primary storage disconnected from backup master
        """
        from neo.master.backup_app import random
        def fetchObjects(orig, min_tid=None, min_oid=ZERO_OID):
            if min_tid is None:
                counts[0] += 1
                if counts[0] > 1:
                    orig.im_self.app.master_conn.close()
            return orig(min_tid, min_oid)
        def onTransactionCommitted(orig, txn):
            counts[0] += 1
            if counts[0] > 1:
                node_list = orig.im_self.nm.getClientList(only_identified=True)
                node_list.remove(txn.getNode())
                node_list[0].getConnection().close()
            return orig(txn)
        upstream = NEOCluster(partitions=4, replicas=0, storage_count=1)
        try:
            upstream.start()
            importZODB = upstream.importZODB(random=random)
            backup = NEOCluster(partitions=4, replicas=2, storage_count=4,
                                upstream=upstream)
            try:
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                backup.tic()
                storage_list = [x.uuid for x in backup.storage_list]
                slave = set(xrange(len(storage_list))).difference
                for event in xrange(10):
                    counts = [0]
                    if event == 5:
                        p = Patch(upstream.master.tm,
                            _on_commit=onTransactionCommitted)
                    else:
                        primary_dict = {}
                        for k, v in sorted(backup.master.backup_app
                                           .primary_partition_dict.iteritems()):
                            primary_dict.setdefault(storage_list.index(v._uuid),
                                                    []).append(k)
                        if event % 2:
                            storage = slave(primary_dict).pop()
                        else:
                            storage, partition_list = primary_dict.popitem()
                        # Populate until the found storage performs
                        # a second replication partially and aborts.
                        p = Patch(backup.storage_list[storage].replicator,
                                  fetchObjects=fetchObjects)
                    try:
                        importZODB(lambda x: counts[0] > 1)
                    finally:
                        del p
                    upstream.client.setPoll(0)
                    backup.tic()
                    self.assertEqual(12, self.checkBackup(backup))
            finally:
                backup.stop()
        finally:
            upstream.stop()

    def testReplicationAbortedBySource(self):
        """
        Check that a feeding node aborts replication when its partition is
        dropped, and that the out-of-date node finishes to replicate from
        another source.
        Here are the different states of partitions over time:
          pt: 0: U|U|U
          pt: 0: UO|UO|UO
          pt: 0: FOO|UO.|U.O # node 1 replicates from node 0
          pt: 0: .OU|UO.|U.O # here node 0 lost partition 0
                             # and node 1 must switch to node 2
          pt: 0: .OU|UO.|U.U
          pt: 0: .OU|UU.|U.U
          pt: 0: .UU|UU.|U.U
        """
        def connected(orig, *args, **kw):
            patch[0] = s1.filterConnection(s0)
            patch[0].add(delayAskFetch,
                Patch(s0.dm, changePartitionTable=changePartitionTable))
            return orig(*args, **kw)
        def delayAskFetch(conn, packet):
            return isinstance(packet, delayed) and packet.decode()[0] == offset
        def changePartitionTable(orig, ptid, cell_list):
            if (offset, s0.uuid, CellStates.DISCARDED) in cell_list:
                patch[0].remove(delayAskFetch)
                # XXX: this is currently not done by
                #      default for performance reason
                orig.im_self.dropPartitions((offset,))
            return orig(ptid, cell_list)
        cluster = NEOCluster(partitions=3, replicas=1, storage_count=3)
        s0, s1, s2 = cluster.storage_list
        for delayed in Packets.AskFetchTransactions, Packets.AskFetchObjects:
            try:
                cluster.start([s0])
                cluster.populate([range(6)] * 3)
                cluster.client.setPoll(0)
                s1.start()
                s2.start()
                cluster.tic()
                cluster.neoctl.enableStorageList([s1.uuid, s2.uuid])
                offset, = [offset for offset, row in enumerate(
                                      cluster.master.pt.partition_list)
                                  for cell in row if cell.isFeeding()]
                patch = [Patch(s1.replicator, fetchTransactions=connected)]
                try:
                    cluster.tic()
                    self.assertEqual(1, patch[0].filtered_count)
                    patch[0]()
                finally:
                    del patch[:]
                cluster.tic()
                self.checkPartitionReplicated(s1, s2, offset)
            finally:
                cluster.stop()
            cluster.reset(True)


if __name__ == "__main__":
    unittest.main()
