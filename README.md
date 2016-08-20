#DSA2

DSA is a distributed storage application which I built from scratch as an exploration in distributed computing and storage. As such, it was built not to be "production" quality software, but rather a good introduction for me and something which is functional for its intended purpose.

At the simplest level, DSA is simply an interface for backing up files from a local file system. These files can then be downloaded again for later use. From the client perspective, file are uploaded and stored in a flat structure on the server, where they are available when needed. 

The architecture of the application is actually more complex and was really the central focus of the project. The system is made up of a single server (named the master controller in the code) and multiple nodes (names slave nodes in the code) connected to the server. The single "server" acts as the interface to clients and recieves requests to upload, download, and delete files located among the slave nodes. It keeps an internal database of all files which have been uploaded and their respective locations. Uploaded files are split into multiple parts and distributed among the slave nodes in the pool with a level of redundancy (the model being RAID 0 + 1). These parts are then downloaded in parallel and merged into a single file upon request by a client. 

Though this module does provide access to the functionality of the software through a command-line interface, which also is used for administrative commands, the software is designed to be included as part of a larger framework as the backend componenent (such as for web server as was done in this case).

This 2nd addition of the project actually uses a reverse architecture from the first. Instead of each slave node initiating the connection to the master upon startup, each slave node instead simply start a server process which waits for incoming requests. The master controller is then responsible for individually adding nodes through their respective ip address and port. In this way, the architecture is more of a single-client, multiple-server structure, but the master controller is still referred to as the server since it interfaces with external clients. 

To configure each node, each must include a file called "config.dsa" in the location of the slave_controller.py file, which is a simple text file in the following format:  

    storage_space    
    storage_loc    
    ip_address    
    port    
    nat[true/false]    
    [nat_ip_address]    
    [nat_port]     

For example:  

    10000000    
    /home/node/storageloc    
    192.168.1.2    
    15890    
    true    
    69.32.5.2    
    15891   
