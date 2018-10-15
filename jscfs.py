import errno
import json
import llfuse
import logging
import os
import stat
import sys
import time
import unittest

from argparse import ArgumentParser

from collections import namedtuple
from grp import getgrnam
from pwd import getpwnam


log = logging.getLogger(__name__)

from enum import Enum
class NodeType(Enum):
    File = 'file'
    Dir = 'directory'


class DirEntry:
    def __init__(self, node):
        self.name = node.name.encode('utf-8')
        self.inode = node.inode

    def __repr__(self):
        s = '({0.name} {0.inode})'.format(self)
        return s

from pwd import getpwnam
from grp import getgrnam

class Node:
    def __init__(self, inode, **kwargs):
        self.inode = inode
        self.name = kwargs.get('name', None)

        self.type = NodeType(kwargs.get('type', None))
        mode = int(kwargs.get('mode', None), 8)
        attr = llfuse.EntryAttributes()
        attr.st_size = 0
        if self.type == NodeType.Dir:
            attr.st_mode = (stat.S_IFDIR | mode)
            self.children = kwargs.get('children', None)
        elif self.type == NodeType.File:
            attr.st_mode = (stat.S_IFREG | mode)
            self.contents = kwargs.get('contents', None)
            if self.contents != None:
                self.contents = self.contents.encode('utf-8')
                attr.st_size = len(self.contents)
        else:
            raise llfuse.FUSEError(errno.ENOENT)

        stamp = int(time.time() * 1e9)
        attr.st_atime_ns = stamp
        attr.st_ctime_ns = stamp
        attr.st_mtime_ns = stamp
        attr.st_uid = getgrnam(kwargs.get('group', None)).gr_gid
        attr.st_gid = getpwnam(kwargs.get('user', None)).pw_uid
        attr.st_ino = inode
        self.attr = attr

    def __repr__(self):
        s = '({1.st_ino} {0.name} {0.type} {1.st_mode:o} {1.st_uid} {1.st_gid})'.format(self,self.attr)
        if self.type == NodeType.Dir and self.children is not None:
            s += "\n\t{}".format(self.children)
        if self.type == NodeType.File and self.contents is not None:
            s += "contents:[{}]".format(self.contents)
        return s


class JsonSysClassFS(llfuse.Operations):
    def __init__(self, json_str ):
        super(JsonSysClassFS, self).__init__()
        self.root_inode = llfuse.ROOT_INODE
        self.superblock = [None] * (self.root_inode + 1)
        self.jdata = json.loads(json_str, object_hook=self.mknode)
        self.fixup_directories()

    def mknode(self, d):
        name = d.get('name', None)
        assert(name != None)
        node = Node(len(self.superblock), **d)
        if node.name == '/':
            node.inode = self.root_inode
            self.superblock[self.root_inode] = node
        else:
            self.superblock.append(node)
        type = d.get('type', None)
        assert(type != None)
        return node

    def fixup_directories(self):
        for inode in self.superblock:
            if inode and inode.type == NodeType.Dir:
                for n, node in enumerate(inode.children):
                    inode.children[n] = DirEntry(node)

    def __repr__(self):
        s = '\n'.join(str(n) for n in self.superblock)
        return s

    # llfuse.Operations start here

    def getattr(self, inode, ctx=None):
        node = self.superblock[inode]
        attr = node.attr
        log.debug('getattr for inode %d:%s', inode, attr)
        return attr

    def lookup(self, dirinode, name, ctx=None):
        log.debug('lookup for %s in %d', name, dirinode)
        dirnode = self.superblock[dirinode]
        assert dirnode.type == NodeType.Dir

        for child in dirnode.children:
            if child.name == name:
                return self.getattr(child.inode)

        raise llfuse.FUSEError(errno.ENOENT)

    def opendir(self, inode, ctx):
        log.debug('opendir %d', inode)
        return inode

    def readdir(self, dirinode, off):
        log.debug('opendir %d starting at %d', dirinode, off)
        dirnode = self.superblock[dirinode]
        assert dirnode.type == NodeType.Dir

        for n, child in enumerate(dirnode.children[off:]):
            yield(child.name, self.getattr(child.inode), n+1)

    def open(self, inode, flags, ctx):
        log.debug('open %d', inode)
        filenode = self.superblock[inode]
        assert filenode.type == NodeType.File
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise llfuse.FUSEError(errno.EPERM)
        return inode

    def release(self, inode):
        pass

    def read(self, inode, off, size):
        log.debug('read %d %d:%d', inode, off, size)
        filenode = self.superblock[inode]
        assert filenode.type == NodeType.File
        if filenode.contents != None:
            return filenode.contents[off:off+size]
        return b''

    def write(self, inode, off, buf):
        log.debug('write %d %d:%s', inode, off, buf)
        filenode = self.superblock[inode]
        assert filenode.type == NodeType.File
        if filenode.contents == None:
            filenode.contents = b''
        filenode.contents = filenode.contents[:offset] + buf + filenode.contents[offset+len(buf):]
        return len(buf)


class TestJscfsMethods(unittest.TestCase):
    test_json_str = """[
   {
      "name" : "/", "type" : "directory",
      "mode" : "0755", "user" : "root", "group" : "root",
      "children" : [
         {
            "name" : "devices", "type" : "directory",
            "mode" : "0755", "user" : "root", "group" : "root",
            "children" : [
               {
                  "name" : "cpu", "type" : "directory",
                  "mode" : "0755", "user" : "root", "group" : "root",
                  "children" : [
                     {
                        "name" : "perf_event_mux_interval_ms", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     },
                     {
                        "name" : "power", "type" : "directory",
                        "mode" : "0755", "user" : "root", "group" : "root",
                        "children" : [
                           {
                              "name" : "runtime_active_kids", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root"
                           },
                           {
                              "name" : "runtime_suspended_time", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root"
                           }
                        ]
                     },
                     {
                        "name" : "uevent", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     }
                  ]
               },
               {
                  "name" : "breakpoint", "type" : "directory",
                  "mode" : "0755", "user" : "root", "group" : "root",
                  "children" : [
                     {
                        "name" : "power", "type" : "directory",
                        "mode" : "0755", "user" : "root", "group" : "root",
                        "children" : [
                           {
                              "name" : "runtime_status", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root",
                              "contents" : "contents of runtime_status"
                           }
                        ]
                     },
                     {
                        "name" : "uevent", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     }
                  ]
               }
            ]
         }
      ]
   }
]
"""
    expectedjdata = [{
      "name" : "/", "type" : "directory",
      "mode" : "0755", "user" : "root", "group" : "root",
      "children" : [
         {
            "name" : "devices", "type" : "directory",
            "mode" : "0755", "user" : "root", "group" : "root",
            "children" : [
               {
                  "name" : "cpu", "type" : "directory",
                  "mode" : "0755", "user" : "root", "group" : "root",
                  "children" : [
                     {
                        "name" : "perf_event_mux_interval_ms", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     },
                     {
                        "name" : "power", "type" : "directory",
                        "mode" : "0755", "user" : "root", "group" : "root",
                        "children" : [
                           {
                              "name" : "runtime_active_kids", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root"
                           },
                           {
                              "name" : "runtime_suspended_time", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root"
                           }
                        ]
                     },
                     {
                        "name" : "uevent", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     }
                  ]
               },
               {
                  "name" : "breakpoint", "type" : "directory",
                  "mode" : "0755", "user" : "root", "group" : "root",
                  "children" : [
                     {
                        "name" : "power", "type" : "directory",
                        "mode" : "0755", "user" : "root", "group" : "root",
                        "children" : [
                           {
                              "name" : "runtime_status", "type" : "file",
                              "mode" : "0444", "user" : "root", "group" : "root",
                              "contents" : "contents of runtime_status"
                           }
                        ]
                     },
                     {
                        "name" : "uevent", "type" : "file",
                        "mode" : "0644", "user" : "root", "group" : "root"
                     }
                  ]
               }
            ]
         }
      ]
   }
]
    def test_json_import(self):
        jdata = json.loads(self.test_json_str)
        self.assertTrue(jdata == self.expectedjdata)

    def test_llfuse_rootinode(self):
        # make sure our assumption about ROOT_INODE is true
        self.assertTrue(llfuse.ROOT_INODE > 0)

    def test_jscfs(self):
        jscfs = JsonSysClassFS(self.test_json_str)
        # there should be as many entries in superblock as json "name" nodes
        # plus one
        self.assertTrue(self.test_json_str.count("name") == 12)
        print(jscfs)

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    return parser.parse_args()


def main():
    options = parse_args()
    init_logging(options.debug)

    testfs = JsonSysClassFS(TestJscfsMethods.test_json_str)
    fuse_options = set(llfuse.default_options)
    fuse_options.add('fsname=jscfs')
    if options.debug_fuse:
        fuse_options.add('debug')
    llfuse.init(testfs, options.mountpoint, fuse_options)
    try:
        llfuse.main(workers=1)
    except:
        llfuse.close(unmount=False)
        raise

    llfuse.close()

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ["-t", "--test"]:
        del(sys.argv[1])
        sys.exit(unittest.main())

    main()
