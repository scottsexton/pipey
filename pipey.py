#!/usr/bin/env python

from Queue import Empty
from ctypes import c_bool
from errno import EPIPE
from multiprocessing import (Process, Queue, Value)
from os import strerror
from random import randint
from time import sleep


def lagify():
    """Module-level function to randomly lag the pipe reads and writes"""
    if randint(0,1):
        sleep(1)


class Pipe(object):
    def __init__(self):
        """These values need to be accessable by both processes"""
        self.pipe = Queue()
        self._closed = Value(c_bool, False, lock=True)

    def write(self, data):
        """Write to the pipe unless it's closed, then raise the appropriate error"""
        if not self.closed:
            self.pipe.put(data)
        else:
            raise IOError("[Errno %s] %s" % (EPIPE, strerror(EPIPE)))

    def read(self):
        """Read from the pipe Queue, this raises Queue.Empty if it's empty"""
        return self.pipe.get(False)

    @property
    def closed(self):
        """Property to allow access to the value of the _closed multiprocessing.Value() object"""
        return self._closed.value

    def close(self):
        """Close the pipe"""
        self._closed.value = True


class Filter(object):
    def __init__(self, send=None, recv=None):
        self.send_pipe = send
        self.recv_pipe = recv


class WriteFilter(Filter):
    def send(self,fh):
        """Loop through the given file and write each line to the pipe"""
        for line in fh:
            self.send_pipe.write(line)
            lagify()

    def __del__(self):
        """When a WriteFilter object is destroyed, close its pipe"""
        self.send_pipe.close()


class CapsFilter(Filter):
    def listen(self):
        """Read from the pipe until it's both empty and closed, calling capitalize() on the data we get"""
        while True:
            try:
                print self.capitalize(self.recv_pipe.read()),
            except Empty:
                if self.recv_pipe.closed:
                    print "Pipe is empty and closed so we can stop listening and return"
                    break
        lagify()

    def capitalize(self, line):
        """Return the uppercase version of the string line"""
        return str(line).upper()

    def send(self,line):
        """CapsFilter could send on another Pipe, or just writes to standard out by default"""
        if self.send_pipe:
            self.send_pipe.write(line)
        else:
            print line


if __name__ == '__main__':
    # Create a WriteFilter with a new Pipe to write to
    writer = WriteFilter(send=Pipe())
    # Create a CapsFilter that listens on the pipe WriteFilter is sending on
    capper = CapsFilter(recv=writer.send_pipe)

    # Start the capper listening to the Pipe on its own process
    print "Starting a CapsFilter listening to the WriteFilter's Pipe"
    pc = Process(target=capper.listen)
    pc.start()

    print "Opening a filehandle to test_data.csv and passing it to the WriteFilter"
    # Get a filehandle to the test data and give it to the writer
    test_data = open('test_data.csv')
    writer.send(test_data)
    print "FileWriter is done sending, closing the pipe"
    writer = None # Cause the Pipe to close
    test_data.close()

    pc.join()
