#
# Copyright (C) 2006-2010  Nexedi SA
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

from struct import pack

import neo
from neo.lib.util import dump
from neo.lib.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from neo.lib.protocol import NotReadyError, ZERO_OID, ZERO_TID
from .handlers import MasterHandler


class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        super(RecoveryManager, self).__init__(app)
        # The target node's uuid to request next.
        self.target_ptid = None
        self.backup_tid_dict = {}

    def getHandler(self):
        return self

    def identifyStorageNode(self, uuid, node):
        """
            Returns the handler for storage nodes
        """
        return uuid, NodeStates.PENDING, self

    def run(self):
        """
        Recover the status about the cluster. Obtain the last OID, the last
        TID, and the last Partition Table ID from storage nodes, then get
        back the latest partition table or make a new table from scratch,
        if this is the first time.
        """
        neo.lib.logging.info('begin the recovery of the status')
        app = self.app
        pt = app.pt
        app.changeClusterState(ClusterStates.RECOVERING)
        pt.setID(None)

        # collect the last partition table available
        poll = app.em.poll
        while 1:
            poll(1)
            allowed_node_set = set()
            if pt.filled():
                # A partition table exists, we are starting an existing
                # cluster.
                partition_node_set = pt.getUpToDateCellNodeSet()
                pending_node_set = set(x for x in partition_node_set
                    if x.isPending())
                if app._startup_allowed or \
                        partition_node_set == pending_node_set:
                    allowed_node_set = pending_node_set
                    extra_node_set = pt.getOutOfDateCellNodeSet()
            elif app._startup_allowed:
                # No partition table and admin allowed startup, we are
                # creating a new cluster out of all pending nodes.
                allowed_node_set = set(app.nm.getStorageList(
                    only_identified=True))
                extra_node_set = set()
            if allowed_node_set:
                for node in allowed_node_set:
                    assert node.isPending(), node
                    if node.getConnection().isPending():
                        break
                else:
                    allowed_node_set |= extra_node_set
                    break

        neo.lib.logging.info('startup allowed')

        for node in allowed_node_set:
            node.setRunning()
        app.broadcastNodesInformation(allowed_node_set)

        if pt.getID() is None:
            neo.lib.logging.info('creating a new partition table')
            # reset IDs generators & build new partition with running nodes
            app.tm.setLastOID(ZERO_OID)
            pt.make(allowed_node_set)
            self._broadcastPartitionTable(pt.getID(), pt.getRowList())
        elif app.backup_tid:
            pt.setBackupTidDict(self.backup_tid_dict)
            app.backup_tid = pt.getBackupTid()

        app.setLastTransaction(app.tm.getLastTID())
        neo.lib.logging.debug(
                        'cluster starts with loid=%s and this partition ' \
                        'table :', dump(app.tm.getLastOID()))
        pt.log()

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node is not None
        if node.getState() == new_state:
            return
        node.setState(new_state)
        # broadcast to all so that admin nodes gets informed
        self.app.broadcastNodesInformation([node])

    def connectionCompleted(self, conn):
        # ask the last IDs to perform the recovery
        conn.ask(Packets.AskLastIDs())

    def answerLastIDs(self, conn, loid, ltid, lptid, backup_tid):
        # Get max values.
        if loid is not None:
            self.app.tm.setLastOID(loid)
        if ltid is not None:
            self.app.tm.setLastTID(ltid)
        if lptid > self.target_ptid:
            # something newer
            self.target_ptid = lptid
            conn.ask(Packets.AskPartitionTable())
        self.backup_tid_dict[conn.getUUID()] = backup_tid

    def answerPartitionTable(self, conn, ptid, row_list):
        if ptid != self.target_ptid:
            # If this is not from a target node, ignore it.
            neo.lib.logging.warn('Got %s while waiting %s', dump(ptid),
                    dump(self.target_ptid))
        else:
            self._broadcastPartitionTable(ptid, row_list)
            self.app.backup_tid = self.backup_tid_dict[conn.getUUID()]

    def _broadcastPartitionTable(self, ptid, row_list):
        try:
            new_nodes = self.app.pt.load(ptid, row_list, self.app.nm)
        except IndexError:
            raise ProtocolError('Invalid offset')
        else:
            notification = Packets.NotifyNodeInformation(new_nodes)
            ptid = self.app.pt.getID()
            row_list = self.app.pt.getRowList()
            partition_table = Packets.SendPartitionTable(ptid, row_list)
            # notify the admin nodes
            for node in self.app.nm.getAdminList(only_identified=True):
                node.notify(notification)
                node.notify(partition_table)
