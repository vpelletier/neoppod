#
# Copyright (C) 2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest
from mock import Mock
from neo import protocol
from neo.protocol import RUNNING_STATE, DOWN_STATE, \
        UNKNOWN_STATE, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, \
        CLIENT_NODE_TYPE, ADMIN_NODE_TYPE
from neo.node import Node, MasterNode, StorageNode, ClientNode, AdminNode, \
        NodeManager
from neo.tests import NeoTestBase
from time import time

class NodesTests(NeoTestBase):

    def setUp(self):
        self.manager = Mock()

    def _updatedByServer(self, node, index=0):
        calls = self.manager.mockGetNamedCalls('unregisterServer')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)
        calls = self.manager.mockGetNamedCalls('registerServer')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)

    def _updatedByUUID(self, node, index=0):
        calls = self.manager.mockGetNamedCalls('unregisterUUID')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)
        calls = self.manager.mockGetNamedCalls('registerUUID')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)

    def testInit(self):
        """ Check the node initialization """
        server = ('127.0.0.1', 10000)
        uuid = self.getNewUUID()
        node = Node(server=server, uuid=uuid)
        self.assertEqual(node.getState(), protocol.UNKNOWN_STATE)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getManager(), None)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time()) 

    def testManager(self):
        """ Check if the node manager is well binded to the node """
        node = Node()
        self.assertEqual(node.getManager(), None)
        node.setManager(self.manager)
        self.assertTrue(node.getManager() is self.manager)
        # XXX: the manager should index the node by uuid and address 

    def testState(self):
        """ Check if the last changed time is updated when state is changed """
        node = Node()
        self.assertEqual(node.getState(), protocol.UNKNOWN_STATE)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())
        previous_time = node.getLastStateChange()
        node.setState(protocol.RUNNING_STATE)
        self.assertEqual(node.getState(), protocol.RUNNING_STATE)
        self.assertTrue(previous_time < node.getLastStateChange())
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())

    def testServer(self):
        """ Check if the manager is updated when a node change it's address """
        node = Node()
        self.assertEqual(node.getServer(), None)
        node.setManager(self.manager)
        server = ('127.0.0.1', 10000)
        node.setServer(server)
        self._updatedByServer(node)

    def testUUID(self):
        """ As for Server but UUID """
        node = Node()
        self.assertEqual(node.getServer(), None)
        node.setManager(self.manager)
        uuid = self.getNewUUID()
        node.setUUID(uuid)
        self._updatedByUUID(node)

    def testTypes(self):
        """ Check that the abstract node has no type """
        node = Node()
        self.assertRaises(NotImplementedError, node.getType)
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testMaster(self):
        """ Check Master sub class """
        node = MasterNode()
        self.assertEqual(node.getType(), protocol.MASTER_NODE_TYPE)
        self.assertTrue(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testStorage(self):
        """ Check Storage sub class """
        node = StorageNode()
        self.assertEqual(node.getType(), protocol.STORAGE_NODE_TYPE)
        self.assertTrue(node.isStorage())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testClient(self):
        """ Check Client sub class """
        node = ClientNode()
        self.assertEqual(node.getType(), protocol.CLIENT_NODE_TYPE)
        self.assertTrue(node.isClient())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isAdmin())

    def testAdmin(self):
        """ Check Admin sub class """
        node = AdminNode()
        self.assertEqual(node.getType(), protocol.ADMIN_NODE_TYPE)
        self.assertTrue(node.isAdmin())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isClient())




class NodeManagerTests(NeoTestBase):

    def setUp(self):
        self.manager = NodeManager()
        self.storage = StorageNode(('127.0.0.1', 1000), self.getNewUUID())
        self.master = MasterNode(('127.0.0.1', 2000), self.getNewUUID())
        self.client = ClientNode(None, self.getNewUUID())
        self.admin = AdminNode(('127.0.0.1', 4000), self.getNewUUID())

    def checkNodes(self, node_list):
        manager = self.manager
        self.assertEqual(sorted(manager.getNodeList()), sorted(node_list))

    def checkMasters(self, master_list):
        manager = self.manager
        self.assertEqual(manager.getMasterNodeList(), master_list)

    def checkStorages(self, storage_list):
        manager = self.manager
        self.assertEqual(manager.getStorageNodeList(), storage_list)

    def checkClients(self, client_list):
        manager = self.manager
        self.assertEqual(manager.getClientNodeList(), client_list)

    def checkByServer(self, node):
        node_found = self.manager.getNodeByServer(node.getServer())
        self.assertEqual(node_found, node)
        
    def checkByUUID(self, node):
        node_found = self.manager.getNodeByUUID(node.getUUID())
        self.assertEqual(node_found, node)

    def testInit(self):
        """ Check the manager is empty when started """
        manager = self.manager
        self.checkNodes([])
        self.checkMasters([])
        self.checkStorages([])
        self.checkClients([])
        server = ('127.0.0.1', 10000)
        self.assertEqual(manager.getNodeByServer(server), None)
        self.assertEqual(manager.getNodeByServer(None), None)
        uuid = self.getNewUUID()
        self.assertEqual(manager.getNodeByUUID(uuid), None)
        self.assertEqual(manager.getNodeByUUID(None), None)

    def testAdd(self):
        """ Check if new nodes are registered in the manager """
        manager = self.manager
        self.checkNodes([])
        # storage
        manager.add(self.storage)
        self.checkNodes([self.storage])
        self.checkStorages([self.storage])
        self.checkMasters([])
        self.checkClients([])
        self.checkByServer(self.storage)
        self.checkByUUID(self.storage)
        # master
        manager.add(self.master)
        self.checkNodes([self.storage, self.master])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([])
        self.checkByServer(self.master)
        self.checkByUUID(self.master)
        # client
        manager.add(self.client)
        self.checkNodes([self.storage, self.master, self.client])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        # client node has no server
        self.assertEqual(manager.getNodeByServer(self.client.getServer()), None)
        self.checkByUUID(self.client)
        # admin
        manager.add(self.admin)
        self.checkNodes([self.storage, self.master, self.client, self.admin])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        self.checkByServer(self.admin)
        self.checkByUUID(self.admin)

    def testClear(self):
        """ Check that the manager clear all its content """
        manager = self.manager
        self.checkNodes([])
        self.checkStorages([])
        self.checkMasters([])
        self.checkClients([])
        manager.add(self.master)
        self.checkMasters([self.master])
        manager.clear()
        self.checkNodes([])
        self.checkMasters([])
        manager.add(self.storage)
        self.checkStorages([self.storage])
        manager.clear()
        self.checkNodes([])
        self.checkStorages([])
        manager.add(self.client)
        self.checkClients([self.client])
        manager.clear()
        self.checkNodes([])
        self.checkClients([])

    def testFilteredClear(self):
        """ Check the clear filter works well """
        manager = self.manager
        manager.add(self.master)
        manager.add(self.storage)
        manager.add(self.client)
        self.checkNodes([self.master, self.storage, self.client])
        drop_clients = lambda node: isinstance(node, ClientNode)
        manager.clear(filter=drop_clients)
        self.checkNodes([self.master, self.storage])
        self.checkClients([])
        drop_masters = lambda node: isinstance(node, MasterNode)
        manager.clear(filter=drop_masters)
        self.checkNodes([self.storage])
        self.checkMasters([])
        drop_storage = lambda node: node is self.storage
        manager.clear(filter=drop_storage)
        self.checkNodes([])
        self.checkStorages([])

    def testUpdate(self):
        """ Check manager content update """
        # set up four nodes
        manager = self.manager
        manager.add(self.master)
        manager.add(self.storage)
        manager.add(self.client)
        manager.add(self.admin)
        self.checkNodes([self.master, self.storage, self.client, self.admin])
        self.checkMasters([self.master])
        self.checkStorages([self.storage])
        self.checkClients([self.client])
        # build changes
        old_address = self.master.getServer()
        new_address = ('127.0.0.1', 2001)
        new_uuid = self.getNewUUID()
        node_list = (
            (CLIENT_NODE_TYPE, None, self.client.getUUID(), DOWN_STATE),
            (MASTER_NODE_TYPE, new_address, self.master.getUUID(), RUNNING_STATE),
            (STORAGE_NODE_TYPE, self.storage.getServer(), new_uuid, RUNNING_STATE),
            (ADMIN_NODE_TYPE, self.admin.getServer(), self.admin.getUUID(), UNKNOWN_STATE),
        )
        # update manager content
        manager.update(node_list)
        # - the client gets down
        self.checkClients([])
        # - master change it's address
        self.checkMasters([self.master])
        self.assertEqual(manager.getNodeByServer(old_address), None)
        self.master.setServer(new_address)
        self.checkByServer(self.master)
        # a new storage replaced the old one
        self.assertNotEqual(manager.getStorageNodeList(), [self.storage])
        self.assertTrue(len(manager.getStorageNodeList()), 1)
        new_storage = manager.getStorageNodeList()[0]
        self.assertEqual(new_storage.getState(), RUNNING_STATE)
        self.assertNotEqual(new_storage, self.storage)
        # admin is still here but in UNKNOWN_STATE
        self.checkNodes([self.master, self.admin, new_storage])
        self.assertEqual(self.admin.getState(), UNKNOWN_STATE)

        
if __name__ == '__main__':
    unittest.main()

