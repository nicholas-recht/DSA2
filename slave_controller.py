import os
import sys
import Pyro4
import base64
if __name__ == "__main__":
    import util
else:
    from . import util

# The node reads a configuration file in the format of:
# storage_space
# storage_loc
# ip_address
# port
# nat[true/false]
# [nat_ip_address]
# [nat_port]

@Pyro4.expose
class Slave:
    def __init__(self):
        # member variables
        self.address = None
        self.port = 0
        self.storage_space = 0
        self.storage_loc = ""
        self.daemon = None
        self.nat_address = None
        self.nat_port = None
        self.nat = False

        # get config settings
        self._read_config_settings()

    def _read_config_settings(self):
        if not os.path.isfile(util.config_file):
            raise FileExistsError()
        file = open(util.config_file, "r")

        self.storage_space = int(file.readline().replace("\n", ""))
        self.storage_loc = file.readline().replace("\n", "")
        self.address = file.readline().replace("\n", "")
        self.port = int(file.readline().replace("\n", ""))

        # nat settings
        nat = file.readline().replace("\n", "")

        if nat.lower() == "true":
            self.nat = True
            self.nat_address = file.readline().replace("\n", "")
            self.nat_port = int(file.readline().replace("\n", ""))
        else:
            self.nat = False

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

    def respond(self):
        return True


def main(args):
    try:
        slave = Slave()

        print("Start daemon")
        with Pyro4.Daemon(nathost=slave.nat_address, natport=slave.nat_port,
                          port=slave.port, host=slave.address) \
                if slave.nat else \
                Pyro4.Daemon(port=slave.port, host=slave.address) as daemon:
            uri = daemon.register(slave, "node")
            slave.daemon = daemon
            print("Daemon started")
            daemon.requestLoop()

    except Exception as e:
        print(str(e))
        # start over again if there is a failure
        main([])

    return


if __name__ == "__main__":
    main(sys.argv)
