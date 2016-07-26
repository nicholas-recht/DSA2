import os
import sys
import socket
import time
import Pyro4
import base64
if __name__ == "__main__":
    import util
else:
    from . import util


@Pyro4.expose
class Slave:
    def __init__(self, address=None, port=None, storage_space=None, storage_loc=None):
        # member variables
        self.id = -1
        self.socket = None
        self.address = None
        self.port = 0
        self.storage_space = 0
        self.storage_loc = ""
        self.daemon = None

        # if all params are None then read from a config file
        if address is None and port is None and storage_space is None and storage_loc is None:
            self._read_config_settings()
        else:
            self.address = address
            self.port = port
            self.storage_space = storage_space
            self.storage_loc = storage_loc

            self._write_config_settings()

        # continually try and create the connection until successful
        while True:
            try:
                self.socket = socket.create_connection((self.address, self.port), util.slave_connect_timeout)
                self.socket.settimeout(None)
                break
            except socket.error as e:
                print(str(e))
                time.sleep(util.slave_connect_wait)
        print("Connection established")

        # initial connection protocol
        self.socket.sendall(util.i_to_bytes(self.id))
        self.id = util.i_from_bytes(self.socket.recv(util.bufsize))

        self._write_config_settings()

    def send_daemon_uri(self, uri):
        self.socket.sendall(util.s_to_bytes(uri))

    def _read_config_settings(self):
        if not os.path.isfile(util.config_file):
            raise FileExistsError()
        file = open(util.config_file, "r")

        self.address = file.readline().replace("\n", "")
        self.port = int(file.readline().replace("\n", ""))
        self.storage_space = int(file.readline().replace("\n", ""))
        self.storage_loc = file.readline().replace("\n", "")
        self.id = int(file.readline().replace("\n", ""))

        file.close()

    def _write_config_settings(self):
        file = open(util.config_file, "w")

        file.write(self.address + "\n")
        file.write(str(self.port) + "\n")
        file.write(str(self.storage_space) + "\n")
        file.write(self.storage_loc + "\n")
        file.write(str(self.id) + "\n")

        file.close()

    def get_storage_space(self):
        return self.storage_space

    def upload_file(self, file_name, params):
        bytes = base64.b64decode(params["data"])
        # write the file
        file = open(self.storage_loc + '/' + file_name, mode='wb')
        file.write(bytes)
        file.close()

        print("File uploaded")

    def delete_file(self, file_name):
        os.remove(self.storage_loc + '/' + file_name)

        print("File deleted")

    def download_file(self, file_name):
        try:
            file = open(self.storage_loc + '/' + file_name, mode='rb')
            f = file.read()
            bytes = bytearray(f)

            file.close()

            print("File downloaded")

            return bytes

        except FileNotFoundError as e:
            print(str(e))

            return None

    def _file_contains_substring(self, path, substr):
        try:
            file = open(self.storage_loc + '/' + path, mode='rb')
            f = file.read()

            if f.find(substr) != -1:
                return True
            else:
                return False

        except Exception as e:
            return False

    def search_files(self, search_string):
        files = os.listdir(self.storage_loc)

        matching = [file for file in files if (self._file_contains_substring(file, search_string))]

        return ','.join(matching)

    def close(self):
        self.daemon.shutdown()


def main(args):
    try:
        # set up the master controller
        if len(args) > 1:
            slave = Slave(args[1], int(args[2]), int(args[3]), args[4])
        else:
            slave = Slave()

        with Pyro4.Daemon(host=socket.gethostbyname(socket.getfqdn())) as daemon:
            uri = daemon.register(slave)
            slave.daemon = daemon
            slave.send_daemon_uri(str(uri))
            daemon.requestLoop()

    except Exception as e:
        print(str(e))
        # start over again if there is a failure
        main([])

    return


if __name__ == "__main__":
    main(sys.argv)
