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
from neo.tests import NeoTestBase
from neo.pt import PartitionTable
from neo.storage.app import Application
from neo.storage.handlers.verification import VerificationHandler
from neo.protocol import CellStates, ErrorCodes
from neo.protocol import INVALID_OID, INVALID_TID
from neo.exception import PrimaryFailure, OperationFailure
from neo.util import p64, u64

class StorageVerificationHandlerTests(NeoTestBase):

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.verification = VerificationHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.client_port = 11011
        self.num_partitions = 1009
        self.num_replicas = 2
        self.app.operational = False
        self.app.load_lock_dict = {}
        self.app.pt = PartitionTable(self.num_partitions, self.num_replicas)


    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    def getClientConnection(self):
        address = ("127.0.0.1", self.client_port)
        return self.getFakeConnection(uuid=self.getNewUUID(), address=address)

    def getMasterConnection(self):
        return self.getFakeConnection(address=("127.0.0.1", self.master_port))

    # Tests
    def test_02_timeoutExpired(self):
        conn = self.getClientConnection()
        self.assertRaises(PrimaryFailure, self.verification.timeoutExpired, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_03_connectionClosed(self):
        conn = self.getClientConnection()
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_04_peerBroken(self):
        conn = self.getClientConnection()
        self.assertRaises(PrimaryFailure, self.verification.peerBroken, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_07_askLastIDs(self):
        conn = self.getClientConnection()
        last_ptid = '\x01' * 8
        last_oid = '\x02' * 8
        self.app.pt = Mock({'getID': last_ptid})
        self.verification.askLastIDs(conn)
        oid, tid, ptid = self.checkAnswerLastIDs(conn, decode=True)
        self.assertEqual(oid, None)
        self.assertEqual(tid, None)
        self.assertEqual(ptid, last_ptid)

        # return value stored in db
        # insert some oid
        conn = self.getClientConnection()
        self.app.dm.begin()
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (3, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (1, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into obj (oid, serial, compression,
        checksum, value) values (2, 'A', 0, 0, '')""")
        self.app.dm.query("""insert into tobj (oid, serial, compression,
        checksum, value) values (5, 'A', 0, 0, '')""")
        # insert some tid
        self.app.dm.query("""insert into trans (tid, oids, user,
                description, ext) values (1, '', '', '', '')""")
        self.app.dm.query("""insert into trans (tid, oids, user,
                description, ext) values (2, '', '', '', '')""")
        self.app.dm.query("""insert into ttrans (tid, oids, user,
                description, ext) values (3, '', '', '', '')""")
        # max tid is in tobj (serial)
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        self.app.dm.commit()
        self.app.dm.setLastOID(last_oid)
        self.verification.askLastIDs(conn)
        self.checkAnswerLastIDs(conn)
        oid, tid, ptid = self.checkAnswerLastIDs(conn, decode=True)
        self.assertEqual(oid, last_oid)
        self.assertEqual(u64(tid), 4)
        self.assertEqual(ptid, self.app.pt.getID())

    def test_08_askPartitionTable(self):
        # try to get unknown offset
        self.assertEqual(len(self.app.pt.getNodeList()), 0)
        self.assertFalse(self.app.pt.hasOffset(1))
        self.assertEqual(len(self.app.pt.getCellList(1)), 0)
        conn = self.getClientConnection()
        self.verification.askPartitionTable(conn, [1])
        ptid, row_list = self.checkAnswerPartitionTable(conn, decode=True)
        self.assertEqual(len(row_list), 1)
        offset, rows = row_list[0]
        self.assertEqual(offset, 1)
        self.assertEqual(len(rows), 0)

        # try to get known offset
        node = self.app.nm.createStorage(
            address=("127.7.9.9", 1),
            uuid=self.getNewUUID()
        )
        self.app.pt.setCell(1, node, CellStates.UP_TO_DATE)
        self.assertTrue(self.app.pt.hasOffset(1))
        conn = self.getClientConnection()
        self.verification.askPartitionTable(conn, [1])
        ptid, row_list = self.checkAnswerPartitionTable(conn, decode=True)
        self.assertEqual(len(row_list), 1)
        offset, rows = row_list[0]
        self.assertEqual(offset, 1)
        self.assertEqual(len(rows), 1)

    def test_10_notifyPartitionChanges(self):
        # old partition change
        conn = self.getMasterConnection()
        self.verification.notifyPartitionChanges(conn, 1, ())
        self.verification.notifyPartitionChanges(conn, 0, ())
        self.assertEqual(self.app.pt.getID(), 1)

        # new node
        conn = self.getMasterConnection()
        new_uuid = self.getNewUUID()
        cell = (0, new_uuid, CellStates.UP_TO_DATE)
        self.app.nm.createStorage(uuid=new_uuid)
        self.app.pt = PartitionTable(1, 1)
        self.app.dm = Mock({ })
        ptid, self.ptid = self.getTwoIDs()
        # pt updated
        self.verification.notifyPartitionChanges(conn, ptid, (cell, ))
        # check db update
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), ptid)
        self.assertEquals(calls[0].getParam(1), (cell, ))

    def test_11_startOperation(self):
        conn = self.getMasterConnection()
        self.assertFalse(self.app.operational)
        self.verification.startOperation(conn)
        self.assertTrue(self.app.operational)

    def test_12_stopOperation(self):
        conn = self.getMasterConnection()
        self.assertRaises(OperationFailure, self.verification.stopOperation, conn)

    def test_13_askUnfinishedTransactions(self):
        # client connection with no data
        conn = self.getMasterConnection()
        self.verification.askUnfinishedTransactions(conn)
        (tid_list, ) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 0)

        # client connection with some data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        self.app.dm.commit()
        conn = self.getMasterConnection()
        self.verification.askUnfinishedTransactions(conn)
        (tid_list, ) = self.checkAnswerUnfinishedTransactions(conn, decode=True)
        self.assertEqual(len(tid_list), 1)
        self.assertEqual(u64(tid_list[0]), 4)

    def test_14_askTransactionInformation(self):
        # ask from client conn with no data
        conn = self.getMasterConnection()
        self.verification.askTransactionInformation(conn, p64(1))
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, ErrorCodes.TID_NOT_FOUND)

        # input some tmp data and ask from client, must find both transaction
        self.app.dm.begin()
        self.app.dm.query("""insert into ttrans (tid, oids, user,
        description, ext) values (3, '%s', 'u1', 'd1', 'e1')""" %(p64(4),))
        self.app.dm.query("""insert into trans (tid, oids, user,
        description, ext) values (1,'%s', 'u2', 'd2', 'e2')""" %(p64(2),))
        self.app.dm.commit()
        # object from trans
        conn = self.getClientConnection()
        self.verification.askTransactionInformation(conn, p64(1))
        tid, user, desc, ext, packed, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 1)
        self.assertEqual(user, 'u2')
        self.assertEqual(desc, 'd2')
        self.assertEqual(ext, 'e2')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 2)
        # object from ttrans
        conn = self.getMasterConnection()
        self.verification.askTransactionInformation(conn, p64(3))
        tid, user, desc, ext, packed, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 3)
        self.assertEqual(user, 'u1')
        self.assertEqual(desc, 'd1')
        self.assertEqual(ext, 'e1')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 4)

        # input some tmp data and ask from server, must find one transaction
        conn = self.getMasterConnection()
        # find the one in trans
        self.verification.askTransactionInformation(conn, p64(1))
        tid, user, desc, ext, packed, oid_list = self.checkAnswerTransactionInformation(conn, decode=True)
        self.assertEqual(u64(tid), 1)
        self.assertEqual(user, 'u2')
        self.assertEqual(desc, 'd2')
        self.assertEqual(ext, 'e2')
        self.assertEqual(len(oid_list), 1)
        self.assertEqual(u64(oid_list[0]), 2)
        # do not find the one in ttrans
        conn = self.getMasterConnection()
        self.verification.askTransactionInformation(conn, p64(2))
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, ErrorCodes.TID_NOT_FOUND)

    def test_15_askObjectPresent(self):
        # client connection with no data
        conn = self.getMasterConnection()
        self.verification.askObjectPresent(conn, p64(1), p64(2))
        code, message = self.checkErrorPacket(conn, decode=True)
        self.assertEqual(code, ErrorCodes.OID_NOT_FOUND)

        # client connection with some data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (1, 2, 0, 0, '')""")
        self.app.dm.commit()
        conn = self.getMasterConnection()
        self.verification.askObjectPresent(conn, p64(1), p64(2))
        oid, tid = self.checkAnswerObjectPresent(conn, decode=True)
        self.assertEqual(u64(tid), 2)
        self.assertEqual(u64(oid), 1)

    def test_16_deleteTransaction(self):
        # client connection with no data
        conn = self.getMasterConnection()
        self.verification.deleteTransaction(conn, p64(1))
        # client connection with data
        self.app.dm.begin()
        self.app.dm.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (1, 2, 0, 0, '')""")
        self.app.dm.commit()
        self.verification.deleteTransaction(conn, p64(2))
        result = self.app.dm.query('select * from tobj')
        self.assertEquals(len(result), 0)

    def test_17_commitTransaction(self):
        # commit a transaction
        conn = self.getMasterConnection()
        dm = Mock()
        self.app.dm = dm
        self.verification.commitTransaction(conn, p64(1))
        self.assertEqual(len(dm.mockGetNamedCalls("finishTransaction")), 1)
        call = dm.mockGetNamedCalls("finishTransaction")[0]
        tid = call.getParam(0)
        self.assertEqual(u64(tid), 1)

if __name__ == "__main__":
    unittest.main()

