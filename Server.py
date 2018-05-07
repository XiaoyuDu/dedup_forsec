'''
NOTES:

This class receives metadata and checks whether or not the files are already availabe. Then it either writes them to an image or send a request for them.

The process is multithreaded.
'''

'''
TO DO:

Work on the client side. Once you get pass sending the files to the server make sure that the image is working properly.
Fragmentation should be dealt with making loop calls to the DD command using the starting and ending blocks... that we will need to get and send with the metadata



Mark:
TODO: Change existing communication method to the new send_msg and recv_msg functions

'''

import subprocess
import datetime
import os
import time
import socket
import threading
import json
import struct
import errno
import subprocess
import re
from threading import Thread
from Queue import Queue
from pymongo import MongoClient
import hashlib

global clientConnected
clientConnected = False

global port_client
global ip_client

global sock_main
global sock_m

global metadata_port_server
global metadata_port_client

global fail_send_file
fail_send_file = []

global current_work_dir
current_work_dir = os.getcwd()

class ThreadedServer(object):
    def __init__(self, host, port):
        try:
            self.host = host
            self.port = port
            self.q_m_alive = True
            self.q_f_alive = True
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.host, self.port))
        except Exception, e:
            print str(e)


    def listen(self):
        self.sock.listen(50)
        while True:
            client, address = self.sock.accept()
            #client.settimeout(60)   # This could be a problem with q_file_workers... the socket cannot die until done.
            threading.Thread(target = self.listenToClient,args = (client,address)).start()

    def threaded_listener(self):
        t_listener = Thread(target=self.listen)
        t_listener.setDaemon(True)
        t_listener.start()
        print 'Listener thread started'

    def listenToClient(self, client, address):
        size = 4096
        db = clientDB.DF_Project_1104
        while 1:

            try:
                data = client.recv(size)
                data_loaded = json.loads(data)
                if data:
                    if type(data_loaded) is not dict:
                        response = 'Data should be json.dumps(dict)'
                        client.send(response)
                        print response + ' -- Received: ' + str(type(data_loaded))
                    else:
                        cmd = str(data_loaded.get('cmd'))

                        if cmd == 'client port':
                            global clientConnected
                            clientConnected = True
                            global port_client
                            global ip_client
                            port_client = data_loaded.get("port_client")
                            ip_client = str(data_loaded.get("ip_client"))
                            print 'port_client' + port_client
                            print 'ip_client' + ip_client
                            response = 'address accepted'
                            client.send(response)
                        elif cmd == 'sending_image_info':

                            db = clientDB.DF_Project_1104
                            image_entry = db.image_acquisition
                            insertion = image_entry.insert_one(
                                {"image_id": data_loaded.get("image_id"),
                                "image_name": data_loaded.get("image_name"),
                                "acquisition_time": data_loaded.get("acquisition_time"),
                                "image_size": data_loaded.get("image_size"),
                                "image_hash": data_loaded.get("image_hash"),
                                 "partition_sha1": data_loaded.get("partition_sha1"),
                                 "partition_size": data_loaded.get("partition_size"),
                                 "partition_name": data_loaded.get("partition_name"),
                                 "dedup_root_path": dedup_root_path

                                 }).inserted_id
                            response = 'Step1 - Got image info'
                            client.send(response)

                        elif cmd == 'new client q_metadata_worker':
                            # global clientConnected
                            clientConnected = True
                            metadata_port_client = data_loaded.get("metadata_port")
                            metadata_worker_thread(metadata_port_client)
                            response = 'Welcome new client q_metadata_worker ' + str(data_loaded.get("worker_num"))
                            client.send(response)

                        elif cmd == 'New client q_file_worker':
                            response = 'Welcome new client q_file_worker ' + str(data_loaded.get("worker_num"))
                            # print response
                            client.send(response)

                        elif cmd == 'Done sending metadata':
                            new_TS.q_m_alive = False
                            print 'metadata transfer finished!!'
                            response = 'Server metadata transfer has finished!!'
                            client.send(response)
                        elif cmd == 'Done sending files':
                            new_TS.q_f_alive = False
                            print 'file transfer finished!!'
                            response = 'Server file transfer has finished!!'
                            client.send(response)
                        elif cmd == "SHUTDOWN":
                            new_TS.q_f_alive = False
                            new_TS.q_m_alive = False
                            print 'all finished~~ shutdown server!!'
                            response = 'Server has been shut down server'
                            client.send(response)

                else:
                    raise NameError('Client disconnected')
            except:
                client.close() #Don't remove/comment out
                return False


# -- FUNCTIONS --


def metadata_worker_thread(port):
    q_m_worker = Thread(target=q_metadata_worker, args=(q_m, port))
    q_m_worker.setDaemon(True)
    q_m_worker.start()
    print '\tq_metadata worker created - ', q_m_worker.name, i + 1

def q_metadata_worker(q_m, metadata_port_client):

    db = clientDB.DF_Project_1104
    metadata_entry = db.metadata_acquisition
    #step 1: connect to socket for metadata
    sock_m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'q_metadata_workder: Got Connected from ', ip_client, metadata_port_client
    sock_m.connect((ip_client, metadata_port_client))

    duzie = 0
    while 1:
        metadata_from_client = recv_msg(sock_m)
        if metadata_from_client is None:
            continue
        else:

            send_msg(sock_m, 'OK')
            q_m.put(metadata_from_client)

            json_metadata_string = json.loads(metadata_from_client)

            try:    # Delete useless CMD
                del json_metadata_string['cmd']
            except KeyError:
                pass

            #Insert Acquisition into DB
            insertion = metadata_entry.insert_one(json_metadata_string).inserted_id

            countOfMetadata = 0
            global verbose
            if verbose:
                print 'Metadata_acquisition: \t' + str(insertion)
            else:
                countOfMetadata = countOfMetadata + 1
                if countOfMetadata % 2 == 0:
                    print "\r|"
                elif countOfMetadata % 3 == 0:
                    print "\r\\"
                elif countOfMetadata % 7 == 0:
                    print "\r/"


            # Check if it's already in MongoDB (Files_collection):
            cursor = db.server_files.find({"sha1": json_metadata_string.get('sha1')}).limit(1)    #server_files: (id, hash, if_path)

            if cursor.count() > 0:  #We already have it
                #print str(insertion) + ' is in the server DB'
                send_msg(sock_m, 'None')
                duzie += 1
                print json_metadata_string.get('sha1'), 'File existing!'
                # cmd_list = prepare_cmd_list(cursor, str(json_metadata_string.get('acquisition_id')+".raw"),
                #     str(json_metadata_string.get('start_block')), str(json_metadata_string.get('number_of_blocks')))
                # q_w.put(cmd_list)
            else:
                #print str(insertion) + ' is NOT in the server DB'
                #       - request from client
                send_msg(sock_m, 'q_f')
                message = {"cmd": 'SERVER file request',
                "files_partition_offset": json_metadata_string.get('partition_offset'),
                "file_fragments": json_metadata_string.get('file_fragments'),
                "inode": json_metadata_string.get('inode'),
                "file_requested": json_metadata_string.get('filepath'),
                "partition_dir":json_metadata_string.get('partition_dir'),
                "mongo_id": str(insertion),
                "filename": json_metadata_string.get('filename'),
                "acquisition_id": json_metadata_string.get('acquisition_id'),
                "sha1": json_metadata_string.get('sha1')}
                q_f_message = json.dumps(message)
                response = send_msg(sock_m, q_f_message)

                if response == 'OK':
                    print metadata_from_client.get('filepath') + ' has been requested'


        if new_TS.q_m_alive == False:
            break

    print 'Terminating q_metadata_worker ', i
    sock_m.close()


def prepare_cmd_list (cursor, image, start_block, number_of_blocks):    #Call this repeteadly to write fragmented files... send the block pairs with metadata.
    for doc in cursor:
        if_path = doc.get('filename')
    aux1 = 'dd'
    aux2 = 'conv=notrunc'
    aux3 = 'if=' + str(if_path)
    aux4 = 'of=' + image
    aux5 = 'bs=4096'    # This should not be hardcoded... we need to adapt to the original image size or we won't be able to get forensically sound.
    aux6 = 'seek=' + start_block
    aux7 = 'count=' + number_of_blocks
    cmd_list = []
    cmd_list.append(aux1)
    cmd_list.append(aux2)
    cmd_list.append(aux3)
    cmd_list.append(aux4)
    cmd_list.append(aux5)
    cmd_list.append(aux6)
    cmd_list.append(aux7)
    return cmd_list



def file_receiver(s,i):
    counter = 0
    aux = 1
    db = clientDB.DF_Project_1104
    file_entry = db.server_files

    # create a new socket for each thread
    s.bind((host_server, 0))  # Bind to the port
    s.listen(5)  # Now wait for connection.

    #send file transfer port to client
    sock_f = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_f.connect((ip_client, port_client))

    port_file_thread = s.getsockname()[1]
    message = {"cmd": 'port from file receiver',
           "port_file_thread": port_file_thread}
    response = send_serialized_message(sock_f, message)
    print response, 'port_file_thread'
    sock_f.close()

    print '--- Server ready to receive a file ---'

    clientSocket, addr = s.accept()  # Establish connection with client.   See about reusing the same socket (problem with message order)
    file_transfer_port = clientSocket.getsockname()[1]

    print 'file_receiver: Got connection from', addr, file_transfer_port

    while True:

        clients_q_read_string = recv_msg(clientSocket)
        if clients_q_read_string is None:
            continue
        else:
            json_clients_read_string = json.loads(clients_q_read_string)  # Dictionary with file info.

            send_msg(clientSocket, "Ok, send file now")

            global filesSubdirectory
            fullSuspectFilePath = json_clients_read_string.get('file_requested')
            partition_dir = json_clients_read_string.get('partition_dir')
            get_db_hash_check = json_clients_read_string.get('sha1')

            dedup_partition_dir = current_work_dir + os.sep + filesSubdirectory  + os.sep + partition_dir
            dedup_partition_dir = dedup_partition_dir.replace(" ", "SPACE")
            try:
                if not os.path.exists(dedup_partition_dir):
                    os.mkdir(dedup_partition_dir)
            except Exception:
                pass

            directories = str(fullSuspectFilePath).split(os.sep)

            artefactPathForDB = current_work_dir + os.sep + filesSubdirectory \
                                + os.sep + partition_dir + os.sep + fullSuspectFilePath
            artefactPathForDB = artefactPathForDB.replace(" ", "SPACE")
            json_clients_read_string['DedupedPath'] = artefactPathForDB

            indexOfLastSlash = int(fullSuspectFilePath.rfind(os.sep))

            # TODO: Recursively create directory structure from Suspect Device on Server
            # runningPath = filesSubdirectory
            dirIndex = 1
            while dirIndex < len(directories) - 1:
                try:
                    dedup_partition_dir += os.sep + directories[dirIndex]
                    if not os.path.isdir(dedup_partition_dir):
                        print 'Attempting to create directory ' + dedup_partition_dir
                        #subprocess.check_output(['mkdir', '-p', os.path.join(filesSubdirectory, fullSuspectFilePath[0:indexOfLastSlash])])
                        os.mkdir(dedup_partition_dir)
                    dirIndex += 1
                except Exception, e:
                    dirIndex += 1
                    print str(e)
            fileToWrite = open( dedup_partition_dir + os.sep + directories[len(directories) - 1], 'wb') # TODO: Use subfolder

            rawFileFromClient = recv_msg(clientSocket)

            #if the file data hash not match, then send it again, until the hash match,data all right.
            t = 0
            while True:
                if t > 2:
                    # fail_send_file.append()
                    print "Fail Tramission file - ", json_clients_read_string.get('filename')
                    break
                check_hash = hashlib.sha1()
                check_hash.update(rawFileFromClient)
                check_hash_hexdigest = check_hash.hexdigest()

                if get_db_hash_check == check_hash_hexdigest:
                    send_msg(clientSocket, "file hash match")
                    print "\tReceiving: " + json_clients_read_string.get('filename')
                    break
                else:
                    send_msg(clientSocket, "send file again")
                    rawFileFromClient = recv_msg(clientSocket)
                t += 1

            fileToWrite.write(rawFileFromClient)

            print "\tDone Receiving: " + json_clients_read_string.get('filename')

            # Insert Acquisition into DB
            try:
                insertion = file_entry.insert_one(json_clients_read_string).inserted_id
                print 'New DB file entry: \t' + str(json_clients_read_string.get('filename'))
            except Exception, e:
                print str(e)

        if new_TS.q_f_alive == False:
            break

    clientSocket.close()  # Close the connection
    print ' Closed connection from', addr

def send_serialized_message(sock, data):    # Consider using encryption.
    data_string = json.dumps(data)
    try:
        sock.sendall(data_string)
        response = sock.recv(4096)
        return response
    except Exception, e:
        print 'ERROR in server' + str(e)


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = ''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


if __name__ == "__main__":

    global verbose
    verbose = False

    global filesSubdirectory
    directoryTimeStamp = str((datetime.datetime.now())).replace(':', '.').replace(' ', '.')
    filesSubdirectory = 'Received_Files' + os.sep + directoryTimeStamp
    try:
        os.mkdir(filesSubdirectory)
    except Exception:
        pass

    host_server = socket.gethostbyname(socket.gethostname())
    port_server = 15554

    # Prepare MongoDB
    print 'Creating MongoDB new client'
    clientDB = MongoClient()

    # Receive files
    subdirectory = "Received_Files"
    try:
        os.mkdir(subdirectory)
    except Exception:
        pass

    # Prepare metadata queue
    q_m = Queue(maxsize=0)
    q_m_num_threads = 5

    q_f_num_threads = 5

    print 'Raising the server listener thread now'
    new_TS = ThreadedServer(host_server, port_server)
    new_TS.threaded_listener()

    while not clientConnected:
        print "Client Not Connected"
        time.sleep(1)

    global sock_main
    sock_main = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_main.connect((ip_client, port_client))
    print 'sock_main created, connected with: => client' ,ip_client, port_client


    cwd = os.getcwd()
    dedup_root_path = cwd + os.sep + filesSubdirectory
    message = {"cmd": 'send_dedup_path', "dedup_path": dedup_root_path}
    response = send_serialized_message(sock_main, message)  # Let server know reading is done
    print "dedup path ~~~", response
    sock_main.close()
    time.sleep(3)


    for i in range(q_f_num_threads):  # Consider raising the q_f_workers after the reading is done.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        file_receiver_worker = Thread(target=file_receiver, args=(s,i))
        file_receiver_worker.setDaemon(True)
        file_receiver_worker.start()
        print 'file_receiver worker created - ', file_receiver_worker.name, i + 1

    print '\t\t\t\tWAIT FOR CLIENT FINISHED!!!'
    while new_TS.q_f_alive == True or new_TS.q_m_alive == True:    # Keep alive
       time.sleep(5)

    print 'SHUTDOWN SERVER!!'