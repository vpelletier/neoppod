#! /usr/bin/env python2.4
#
# neomaster - run a master node of NEO
#
# Copyright (C) 2006  Nexedi SA
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

from optparse import OptionParser
import logging
import time
import os

from neo.lib import setupLog

# register options
parser = OptionParser()
parser.add_option('-v', '--verbose', action = 'store_true', 
                  help = 'print verbose messages')
parser.add_option('-s', '--source', help = 'the source database')
parser.add_option('-d', '--destination', help = 'the destination database')
parser.add_option('-c', '--cluster', help = 'the NEO cluster name')

def main(args=None):
    # parse options
    (options, args) = parser.parse_args(args=args)
    source = options.source or None
    destination = options.destination or None
    cluster = options.cluster or None

    # check options
    if source is None or destination is None:
        raise RuntimeError('Source and destination databases must be supplied')
    if cluster is None:
        raise RuntimeError('The NEO cluster name must be supplied')

    # set up logging
    setupLog('NEOMIGRATE', None, options.verbose or False)

    # open storages
    from ZODB.FileStorage import FileStorage
    #from ZEO.ClientStorage import ClientStorage as ZEOStorage
    from neo.client.Storage import Storage as NEOStorage
    if os.path.exists(source):
        src = FileStorage(file_name=source)
        dst = NEOStorage(master_nodes=destination, name=cluster)
    else:
        src = NEOStorage(master_nodes=source, name=cluster)
        dst = FileStorage(file_name=destination)

    # do the job
    print "Migrating from %s to %s" % (source, destination)
    start = time.time()
    dst.copyTransactionsFrom(src, verbose=0)
    elapsed = time.time() - start
    print "Migration done in %3.5f" % (elapsed, )

