#
# Copyright (C) 2006-2009  Nexedi SA
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

from neo import logging

from neo.storage.handlers import BaseStorageHandler
from neo import protocol
from neo.util import dump
from neo.node import ClientNode

class IdentificationHandler(BaseStorageHandler):
    """ Handler used for incoming connections during operation state """

    def connectionLost(self, conn, new_state):
        logging.warning('A connection was lost during identification')

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        self.checkClusterName(name)
        # reject any incoming connections if not ready
        if not self.app.ready:
            raise protocol.NotReadyError
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # choose the handler according to the node type
        if node_type == protocol.CLIENT_NODE_TYPE:
            from neo.storage.handlers.client import ClientOperationHandler 
            handler = ClientOperationHandler
            if node is None:
                node = ClientNode()
                app.nm.add(node)
        elif node_type == protocol.STORAGE_NODE_TYPE:
            from neo.storage.handlers.storage import StorageOperationHandler
            handler = StorageOperationHandler
        else:
            raise protocol.ProtocolError('reject non-client-or-storage node')
        if node is None:
            logging.error('reject an unknown node %s', dump(uuid))
            raise protocol.NotReadyError
        # If this node is broken, reject it.
        if node.getState() == protocol.BROKEN_STATE:
            raise protocol.BrokenNodeDisallowedError
        # apply the handler and set up the connection
        handler = handler(self.app)
        conn.setHandler(handler)
        conn.setUUID(uuid)
        node.setUUID(uuid)
        args = (protocol.STORAGE_NODE_TYPE, app.uuid, app.server, 
                app.pt.getPartitions(), app.pt.getReplicas(), uuid)
        # accept the identification and trigger an event
        conn.answer(protocol.acceptNodeIdentification(*args), packet.getId())
        handler.connectionCompleted(conn)

