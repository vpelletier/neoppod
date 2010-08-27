#
# Copyright (C) 2009-2010  Nexedi SA
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

import unittest
from mock import Mock
from struct import pack
from neo.tests import NeoTestBase
from neo.protocol import NodeTypes, NodeStates
from neo.master.handlers.storage import StorageServiceHandler
from neo.master.handlers.client import ClientServiceHandler
from neo.master.app import Application
from neo.exception import OperationFailure

class MasterStorageHandlerTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.pt.clear()
        self.app.pt.setID(pack('!Q', 1))
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

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def _allocatePort(self):
        self.port = getattr(self, 'port', 1000) + 1
        return self.port

    def _getClient(self):
        return self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                ip='127.0.0.1', port=self._allocatePort())

    def _getStorage(self):
        return self.identifyToMasterNode(node_type=NodeTypes.STORAGE,
                ip='127.0.0.1', port=self._allocatePort())

    def getLastUUID(self):
        return self.uuid

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        nm = self.app.nm
        uuid = self.getNewUUID()
        node = nm.createFromNodeType(node_type, address=(ip, port),
                uuid=uuid)
        conn = self.getFakeConnection(node.getUUID(),node.getAddress())
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
        tid = self.app.tm.begin()
        self.app.tm.prepare(client_1, tid, oid_list, uuid_list, msg_id)
        self.assertTrue(tid in self.app.tm)
        # the first storage acknowledge the lock
        self.service.answerInformationLocked(storage_conn_1, tid)
        self.checkNoPacketSent(client_conn_1)
        self.checkNoPacketSent(client_conn_2)
        self.checkNoPacketSent(storage_conn_1)
        self.checkNoPacketSent(storage_conn_2)
        # then the second
        self.service.answerInformationLocked(storage_conn_2, tid)
        self.checkAnswerTransactionFinished(client_conn_1)
        self.checkInvalidateObjects(client_conn_2)
        self.checkNotifyUnlockInformation(storage_conn_1)
        self.checkNotifyUnlockInformation(storage_conn_2)

    def test_12_askLastIDs(self):
        service = self.service
        node, conn = self.identifyToMasterNode()
        # give a uuid
        conn = self.getFakeConnection(node.getUUID(), self.storage_address)
        ptid = self.app.pt.getID()
        oid = self.getOID(1)
        tid = self.getNextTID()
        self.app.tm.setLastOID(oid)
        self.app.tm.setLastTID(tid)
        service.askLastIDs(conn)
        packet = self.checkAnswerLastIDs(conn)
        loid, ltid, lptid = packet.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)

    def test_13_askUnfinishedTransactions(self):
        service = self.service
        node, conn = self.identifyToMasterNode()
        # give a uuid
        service.askUnfinishedTransactions(conn)
        packet = self.checkAnswerUnfinishedTransactions(conn)
        tid_list, = packet.decode()
        self.assertEqual(tid_list, [])
        # create some transaction
        node, conn = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        def create_transaction(index):
            tid = self.getNextTID()
            oid_list = [self.getOID(index)]
            self.app.tm.prepare(node, tid, oid_list, [node.getUUID()], index)
        create_transaction(1)
        create_transaction(2)
        create_transaction(3)
        conn = self.getFakeConnection(node.getUUID(), self.storage_address)
        service.askUnfinishedTransactions(conn)
        packet = self.checkAnswerUnfinishedTransactions(conn)
        (tid_list, ) = packet.decode()
        self.assertEqual(len(tid_list), 3)

    def _testWithMethod(self, method, state):
        # define two nodes
        node1, conn1 = self.identifyToMasterNode()
        node2, conn2 = self.identifyToMasterNode()
        node1.setRunning()
        node2.setRunning()
        self.assertEquals(node1.getState(), NodeStates.RUNNING)
        self.assertEquals(node2.getState(), NodeStates.RUNNING)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        # drop one node
        lptid = self.app.pt.getID()
        method(conn1)
        self.assertEquals(node1.getState(), state)
        self.assertTrue(lptid < self.app.pt.getID())
        # drop the second, no storage node left
        lptid = self.app.pt.getID()
        self.assertEquals(node2.getState(), NodeStates.RUNNING)
        self.assertRaises(OperationFailure, method, conn2)
        self.assertEquals(node2.getState(), state)
        self.assertEquals(lptid, self.app.pt.getID())

    def test_15_peerBroken(self):
        self._testWithMethod(self.service.peerBroken, NodeStates.BROKEN)

    def test_16_timeoutExpired(self):
        self._testWithMethod(self.service.timeoutExpired,
                NodeStates.TEMPORARILY_DOWN)

    def test_17_connectionClosed(self):
        self._testWithMethod(self.service.connectionClosed,
                NodeStates.TEMPORARILY_DOWN)

    def test_nodeLostAfterAskLockInformation(self):
        # 2 storage nodes, one will die
        node1, conn1 = self._getStorage()
        node2, conn2 = self._getStorage()
        # client nodes, to distinguish answers for the sample transactions
        client1, cconn1 = self._getClient()
        client2, cconn2 = self._getClient()
        client3, cconn3 = self._getClient()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        tid3 = self.getNextTID(tid2)
        oid_list = [self.getOID(), ]

        # Some shortcuts to simplify test code
        self.app.pt = Mock({'operational': True})
        self.app.outdateAndBroadcastPartition = lambda: None

        # Register some transactions
        tm = self.app.tm
        # Transaction 1: 2 storage nodes involved, one will die and the other
        # already answered node lock
        msg_id_1 = 1
        tm.prepare(client1, tid1, oid_list, [node1.getUUID(), node2.getUUID()], msg_id_1)
        tm.lock(tid1, node2.getUUID())
        # Transaction 2: 2 storage nodes involved, one will die
        msg_id_2 = 2
        tm.prepare(client2, tid2, oid_list, [node1.getUUID(), node2.getUUID()], msg_id_2)
        # Transaction 3: 1 storage node involved, which won't die
        msg_id_3 = 3
        tm.prepare(client3, tid3, oid_list, [node2.getUUID(), ], msg_id_3)

        # Assert initial state
        self.checkNoPacketSent(cconn1)
        self.checkNoPacketSent(cconn2)
        self.checkNoPacketSent(cconn3)

        # Storage 1 dies
        node1.setTemporarilyDown()
        self.service.nodeLost(conn1, node1)

        # Check state after node lost
        # T1: last locking node lost, client receives AnswerTransactionFinished
        self.checkAnswerTransactionFinished(cconn1)
        # ...and notifications are sent to other clients
        self.checkInvalidateObjects(cconn2)
        self.checkInvalidateObjects(cconn3)
        # T2: pending locking answer, client keeps waiting
        self.checkNoPacketSent(cconn2, check_notify=False)
        # T3: action not significant to this transacion, so no response
        self.checkNoPacketSent(cconn3, check_notify=False)

if __name__ == '__main__':
    unittest.main()

