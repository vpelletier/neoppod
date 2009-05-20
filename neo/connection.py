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

import logging
from neo.locking import RLock
import sys
import traceback

from neo import protocol
from neo.protocol import Packet, PacketMalformedError
from neo.event import IdleEvent
from neo.connector import ConnectorException, ConnectorTryAgainException, \
        ConnectorInProgressException, ConnectorConnectionRefusedException
from neo.util import dump
from neo.exception import OperationFailure

def lockCheckWrapper(func):
    """
    This function is to be used as a wrapper around
    MT(Client|Server)Connection class methods.

    It uses a "_" method on RLock class, so it might stop working without
    notice (sadly, RLock does not offer any "acquired" method, but that one
    will do as it checks that current thread holds this lock).

    It requires moniroted class to have an RLock instance in self._lock
    property.
    """
    def wrapper(self, *args, **kw):
        if not self._lock._is_owned():
            import traceback
            logging.warning('%s called on %s instance without being locked. Stack:\n%s', func.func_code.co_name, self.__class__.__name__, ''.join(traceback.format_stack()))
        # Call anyway
        return func(self, *args, **kw)
    return wrapper

class BaseConnection(object):
    """A base connection."""

    def __init__(self, event_manager, handler, connector = None,
                 addr = None, connector_handler = None):
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self.handler = handler
        if connector is not None:
            self.connector_handler = connector.__class__
            event_manager.register(self)
        else:            
            self.connector_handler = connector_handler
            
    def lock(self):
        return 1

    def unlock(self):
        return None

    def getConnector(self):
        return self.connector

    def getDescriptor(self):
        return self.connector.getDescriptor()

    def setConnector(self, connector):
        if self.connector is not None:
            raise RuntimeError, 'cannot overwrite a connector in a connection'
        if connector is not None:
            self.connector = connector
            self.em.register(self)

    def getAddress(self):
        return self.addr

    def readable(self):
        raise NotImplementedError

    def writable(self):
        raise NotImplementedError

    def getHandler(self):
        return self.handler

    def setHandler(self, handler):
        self.handler = handler

    def getEventManager(self):
        return self.em

    def getUUID(self):
        return None

    def isListeningConnection(self):
        raise NotImplementedError

class ListeningConnection(BaseConnection):
    """A listen connection."""
    def __init__(self, event_manager, handler, addr = None,
                 connector_handler = None, **kw):
        logging.info('listening to %s:%d', *addr)
        BaseConnection.__init__(self, event_manager, handler,
                                addr = addr,
                                connector_handler = connector_handler)
        connector = self.connector_handler()
        connector.makeListeningConnection(addr)
        self.setConnector(connector)
        self.em.addReader(self)

    def readable(self):
        try:
            new_s, addr = self.connector.getNewConnection()
            logging.info('accepted a connection from %s:%d', *addr)
            self.handler.connectionAccepted(self, new_s, addr)
        except ConnectorTryAgainException:
            pass
            
    def isListeningConnection(self):
        return True

class Connection(BaseConnection):
    """A connection."""
    def __init__(self, event_manager, handler,
                 connector = None, addr = None,
                 connector_handler = None):
        self.read_buf = ""
        self.write_buf = ""
        self.cur_id = 0
        self.event_dict = {}
        self.aborted = False
        self.uuid = None
        BaseConnection.__init__(self, event_manager, handler,
                                connector = connector, addr = addr,
                                connector_handler = connector_handler)
        if connector is not None:
            event_manager.addReader(self)

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        self.uuid = uuid

    def getNextId(self):
        next_id = self.cur_id
        # Deal with an overflow.
        if self.cur_id == 0xffffffff:
            self.cur_id = 0
        else:
            self.cur_id += 1
        return next_id

    def close(self):
        """Close the connection."""
        em = self.em
        if self.connector is not None:
            logging.debug('closing a connector for %s (%s:%d)', 
                    dump(self.uuid), *(self.addr))
            em.removeReader(self)
            em.removeWriter(self)
            em.unregister(self)            
            self.connector.shutdown()
            self.connector.close()
            self.connector = None
            for event in self.event_dict.itervalues():
                em.removeIdleEvent(event)
            self.event_dict.clear()

    def __del__(self):
        self.close()

    def abort(self):
        """Abort dealing with this connection."""
        logging.debug('aborting a connector for %s (%s:%d)', 
                dump(self.uuid), *(self.addr))
        self.aborted = True

    def writable(self):
        """Called when self is writable."""
        self.send()
        if not self.pending():
            if self.aborted:
                self.close()
            else:
                self.em.removeWriter(self)

    def readable(self):
        """Called when self is readable."""
        self.recv()
        self.analyse()

        if self.aborted:
            self.em.removeReader(self)

    def analyse(self):
        """Analyse received data."""
        while 1:
            packet = None
            try:
                packet = protocol.parse(self.read_buf)
            except PacketMalformedError, msg:
                self.handler.packetMalformed(self, packet, msg)
                return

            if packet is None:
                break

            # Remove idle events, if appropriate packets were received.
            for msg_id in (None, packet.getId()):
                try:
                    event = self.event_dict[msg_id]
                    del self.event_dict[msg_id]
                    self.em.removeIdleEvent(event)
                except KeyError:
                    pass

            logging.debug('#0x%04x %-30s from %s (%s:%d)', packet.getId(), 
                    packet.getType(), dump(self.uuid), *self.getAddress())

            try:
                self.handler.packetReceived(self, packet)
            finally:
                self.read_buf = self.read_buf[len(packet):]

    def pending(self):
        return self.connector is not None and self.write_buf

    def recv(self):
        """Receive data from a connector."""
        try:
            data = self.connector.receive()
            if not data:
                self.handler.connectionClosed(self)
                self.close()
                return
            self.read_buf += data
        except ConnectorTryAgainException:        
            pass
        except ConnectorConnectionRefusedException:
            # should only occur while connecting
            assert self.connecting
            self.handler.connectionFailed(self)
            self.close()
        except ConnectorException:
            self.handler.connectionClosed(self)
            self.close()
            # unhandled connector exception
            raise

    def send(self):
        """Send data to a connector."""
        if not self.write_buf:
            return
        try:
            n = self.connector.send(self.write_buf)
            if not n:
                self.handler.connectionClosed(self)
                self.close()
                return
            self.write_buf = self.write_buf[n:]
        except ConnectorTryAgainException:
            pass
        except ConnectorException:
            self.handler.connectionClosed(self)
            self.close()
            # unhandled connector exception
            raise 

    def addPacket(self, packet):
        """Add a packet into the write buffer."""
        if self.connector is None:
            return

        logging.debug('#0x%04x %-30s  to  %s (%s:%d)', packet.getId(),
                packet.getType(), dump(self.uuid), *self.getAddress())
        try:
            self.write_buf += packet.encode()
        except PacketMalformedError, m:
            logging.critical('trying to send a too big message')
            # XXX: we should assert that the internalError packet has a size
            # lower than MAX_PACKET_SIZE
            return self.notify(protocol.internalError(m))

        # If this is the first time, enable polling for writing.
        if self.write_buf:
            self.em.addWriter(self)

    def expectMessage(self, msg_id = None, timeout = 5, additional_timeout = 30):
        """Expect a message for a reply to a given message ID or any message.

        The purpose of this method is to define how much amount of time is
        acceptable to wait for a message, thus to detect a down or broken
        peer. This is important, because one error may halt a whole cluster
        otherwise. Although TCP defines a keep-alive feature, the timeout
        is too long generally, and it does not detect a certain type of reply,
        thus it is better to probe problems at the application level.

        The message ID specifies what ID is expected. Usually, this should
        be identical with an ID for a request message. If it is None, any
        message is acceptable, so it can be used to check idle time.

        The timeout is the amount of time to wait until keep-alive messages start.
        Once the timeout is expired, the connection starts to ping the peer.

        The additional timeout defines the amount of time after the timeout
        to invoke a timeoutExpired callback. If it is zero, no ping is sent, and
        the callback is executed immediately."""
        if self.connector is None:
            return

        event = IdleEvent(self, msg_id, timeout, additional_timeout)
        self.event_dict[msg_id] = event
        self.em.addIdleEvent(event)

    def notify(self, packet):
        msg_id = self.getNextId()
        packet.setId(msg_id)
        self.addPacket(packet)
        return msg_id

    def ask(self, packet, timeout=5, additional_timeout=30):
        msg_id = self.getNextId()
        packet.setId(msg_id)
        self.expectMessage(msg_id)
        self.addPacket(packet)
        return msg_id

    def answer(self, packet, answer_to):
        msg_id = answer_to.getId()
        packet.setId(msg_id)
        self.addPacket(packet)

    def isServerConnection(self):
        raise NotImplementedError

    def isListeningConnection(self):
        return False

class ClientConnection(Connection):
    """A connection from this node to a remote node."""
    def __init__(self, event_manager, handler, addr = None,
                 connector_handler = None, **kw):
        self.connecting = True
        Connection.__init__(self, event_manager, handler, addr = addr,
                            connector_handler = connector_handler)
        handler.connectionStarted(self)
        try:
            connector = self.connector_handler()
            self.setConnector(connector)
            try:
                connector.makeClientConnection(addr)
            except ConnectorInProgressException:
                event_manager.addWriter(self)
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                event_manager.addReader(self)
        except ConnectorConnectionRefusedException:
            handler.connectionFailed(self)
            self.close()
        except ConnectorException, msg:
            # unhandled connector exception
            handler.connectionFailed(self)
            self.close()
            raise

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.connector.getError()
            if err:
                self.handler.connectionFailed(self)
                self.close()
                return
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                self.em.addReader(self)
        else:
            Connection.writable(self)

    def isServerConnection(self):
        return False

class ServerConnection(Connection):
    """A connection from a remote node to this node."""
    def isServerConnection(self):
        return True

class MTClientConnection(ClientConnection):
    """A Multithread-safe version of ClientConnection."""
    def __init__(self, *args, **kwargs):
        # _lock is only here for lock debugging purposes. Do not use.
        self._lock = lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        self.lock()
        try:
            super(MTClientConnection, self).__init__(*args, **kwargs)
        finally:
            self.unlock()

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

    @lockCheckWrapper
    def recv(self, *args, **kw):
        return super(MTClientConnection, self).recv(*args, **kw)

    @lockCheckWrapper
    def send(self, *args, **kw):
        return super(MTClientConnection, self).send(*args, **kw)

    @lockCheckWrapper
    def writable(self, *args, **kw):
        return super(MTClientConnection, self).writable(*args, **kw)

    @lockCheckWrapper
    def readable(self, *args, **kw):
        return super(MTClientConnection, self).readable(*args, **kw)

    @lockCheckWrapper
    def analyse(self, *args, **kw):
        return super(MTClientConnection, self).analyse(*args, **kw)

    @lockCheckWrapper
    def addPacket(self, *args, **kw):
        return super(MTClientConnection, self).addPacket(*args, **kw)

    @lockCheckWrapper
    def getNextId(self, *args, **kw):
        return super(MTClientConnection, self).getNextId(*args, **kw)

    @lockCheckWrapper
    def expectMessage(self, *args, **kw):
        return super(MTClientConnection, self).expectMessage(*args, **kw)

class MTServerConnection(ServerConnection):
    """A Multithread-safe version of ServerConnection."""
    def __init__(self, *args, **kwargs):
        # _lock is only here for lock debugging purposes. Do not use.
        self._lock = lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        self.lock()
        try:
            super(MTServerConnection, self).__init__(*args, **kwargs)
        finally:
            self.unlock()

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

    @lockCheckWrapper
    def recv(self, *args, **kw):
        return super(MTServerConnection, self).recv(*args, **kw)

    @lockCheckWrapper
    def send(self, *args, **kw):
        return super(MTServerConnection, self).send(*args, **kw)

    @lockCheckWrapper
    def writable(self, *args, **kw):
        return super(MTServerConnection, self).writable(*args, **kw)

    @lockCheckWrapper
    def readable(self, *args, **kw):
        return super(MTServerConnection, self).readable(*args, **kw)

    @lockCheckWrapper
    def analyse(self, *args, **kw):
        return super(MTServerConnection, self).analyse(*args, **kw)

    @lockCheckWrapper
    def addPacket(self, *args, **kw):
        return super(MTServerConnection, self).addPacket(*args, **kw)

    @lockCheckWrapper
    def getNextId(self, *args, **kw):
        return super(MTServerConnection, self).getNextId(*args, **kw)

    @lockCheckWrapper
    def expectMessage(self, *args, **kw):
        return super(MTServerConnection, self).expectMessage(*args, **kw)

