#
# Copyright (C) 2009-2016  Nexedi SA
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

import unittest
from mock import Mock
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, NodeStates, Packets
from neo.master.handlers.storage import StorageServiceHandler
from neo.master.handlers.client import ClientServiceHandler
from neo.master.app import Application
from neo.lib.exception import StoppedOperation

class MasterStorageHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.em = Mock()
        self.service = StorageServiceHandler(self.app)
        self.client_handler = ClientServiceHandler(self.app)
        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10010
        self.master_address = ('127.0.0.1', self.master_port)
        self.client_address = ('127.0.0.1', self.client_port)
        self.storage_address = ('127.0.0.1', self.storage_port)

    def _allocatePort(self):
        self.port = getattr(self, 'port', 1000) + 1
        return self.port

    def _getClient(self):
        return self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                ip='127.0.0.1', port=self._allocatePort())

    def _getStorage(self):
        return self.identifyToMasterNode(node_type=NodeTypes.STORAGE,
                ip='127.0.0.1', port=self._allocatePort())

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        nm = self.app.nm
        uuid = self.getNewUUID(node_type)
        node = nm.createFromNodeType(node_type, address=(ip, port),
                uuid=uuid)
        conn = self.getFakeConnection(node.getUUID(), node.getAddress())
        node.setConnection(conn)
        return (node, conn)

    def test_answerInformationLocked_1(self):
        """
            Master must refuse to lock if the TID is greater than the last TID
        """
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        self.app.tm.setLastTID(tid1)
        self.assertTrue(tid1 < tid2)
        node, conn = self.identifyToMasterNode()
        self.checkProtocolErrorRaised(self.service.answerInformationLocked,
                conn, tid2)
        self.checkNoPacketSent(conn)

    def test_answerInformationLocked_2(self):
        """
            Master must:
            - lock each storage
            - notify the client
            - invalidate other clients
            - unlock storages
        """
        # one client and two storages required
        client_1, client_conn_1 = self._getClient()
        client_2, client_conn_2 = self._getClient()
        storage_1, storage_conn_1 = self._getStorage()
        storage_2, storage_conn_2 = self._getStorage()
        uuid_list = storage_1.getUUID(), storage_2.getUUID()
        oid_list = self.getOID(), self.getOID()
        msg_id = 1
        # register a transaction
        ttid = self.app.tm.begin(client_1)
        tid = self.app.tm.prepare(ttid, 1, oid_list, uuid_list,
            msg_id)
        self.assertTrue(ttid in self.app.tm)
        # the first storage acknowledge the lock
        self.service.answerInformationLocked(storage_conn_1, ttid)
        self.checkNoPacketSent(client_conn_1)
        self.checkNoPacketSent(client_conn_2)
        self.checkNoPacketSent(storage_conn_1)
        self.checkNoPacketSent(storage_conn_2)
        # then the second
        self.service.answerInformationLocked(storage_conn_2, ttid)
        self.checkAnswerTransactionFinished(client_conn_1)
        self.checkInvalidateObjects(client_conn_2)
        self.checkNotifyUnlockInformation(storage_conn_1)
        self.checkNotifyUnlockInformation(storage_conn_2)

    def test_13_askUnfinishedTransactions(self):
        service = self.service
        node, conn = self.identifyToMasterNode()
        # give a uuid
        service.askUnfinishedTransactions(conn)
        packet = self.checkAnswerUnfinishedTransactions(conn)
        max_tid, tid_list = packet.decode()
        self.assertEqual(tid_list, [])
        # create some transaction
        node, conn = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        ttid = self.app.tm.begin(node)
        self.app.tm.prepare(ttid, 1,
            [self.getOID(1)], [node.getUUID()], 1)
        conn = self.getFakeConnection(node.getUUID(), self.storage_address)
        service.askUnfinishedTransactions(conn)
        max_tid, tid_list = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 1)

    def test_connectionClosed(self):
        method = self.service.connectionClosed
        state = NodeStates.TEMPORARILY_DOWN
        # define two nodes
        node1, conn1 = self.identifyToMasterNode()
        node2, conn2 = self.identifyToMasterNode(port=10022)
        node1.setRunning()
        node2.setRunning()
        self.assertEqual(node1.getState(), NodeStates.RUNNING)
        self.assertEqual(node2.getState(), NodeStates.RUNNING)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        # drop one node
        lptid = self.app.pt.getID()
        method(conn1)
        self.assertEqual(node1.getState(), state)
        self.assertTrue(lptid < self.app.pt.getID())
        # drop the second, no storage node left
        lptid = self.app.pt.getID()
        self.assertEqual(node2.getState(), NodeStates.RUNNING)
        self.assertRaises(StoppedOperation, method, conn2)
        self.assertEqual(node2.getState(), state)
        self.assertEqual(lptid, self.app.pt.getID())

    def test_answerPack(self):
        # Note: incomming status has no meaning here, so it's left to False.
        node1, conn1 = self._getStorage()
        node2, conn2 = self._getStorage()
        self.app.packing = None
        # Does nothing
        self.service.answerPack(None, False)

        client_conn = Mock({
            'getPeerId': 512,
        })
        client_peer_id = 42
        self.app.packing = (client_conn, client_peer_id,
                            {conn1.getUUID(), conn2.getUUID()})
        self.service.answerPack(conn1, False)
        self.checkNoPacketSent(client_conn)
        self.assertEqual(self.app.packing[2], {conn2.getUUID()})
        self.service.answerPack(conn2, False)
        status = self.checkAnswerPacket(client_conn, Packets.AnswerPack,
            decode=True)[0]
        # TODO: verify packet peer id
        self.assertTrue(status)
        self.assertEqual(self.app.packing, None)

    def test_notifyReady(self):
        node, conn = self._getStorage()
        uuid = node.getUUID()
        self.assertFalse(self.app.isStorageReady(uuid))
        self.service.notifyReady(conn)
        self.assertTrue(self.app.isStorageReady(uuid))

if __name__ == '__main__':
    unittest.main()

