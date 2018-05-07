

from unallocated_space import get_continuous_unallocated_block, extract_unallocated_data
from drive_image import generate_image_sha1, generate_part_sha1
from extract_special_files import dd_extract_file, dd_extract_file_by_extension, dd_extract_duplicated_file_slack
import hashlib
import datetime
import pytsk3, pyewf
import time
import socket
import threading
import json
import csv
import pyvhdi
import struct
import os, sys
import argparse

from threading import Thread
from Queue import Queue

global noOfFilesSent
noOfFilesSent = 0
global transmittedFileSize
transmittedFileSize = 0
global diskCumulativeFileSize
diskCumulativeFileSize = 0
global noOfFilesOnDisk
noOfFilesOnDisk = 0
global metadataThreadsAlive
metadataThreadsAlive = 0
global fileThreadsAlive
fileThreadsAlive = 0

global image_data_dedup_path
image_data_dedup_path = ''

global sock_main

global file_worker_count
file_worker_count = 0

global image_size
image_size = 0

global metadata_prot_server
global metadata_port_client

global current_work_dir
current_work_dir = os.getcwd()


global sector_size
sector_size = 512

global drive_block_list
drive_block_list = []

class ewf_Img_Info(pytsk3.Img_Info):
  def __init__(self, ewf_handle):
    self._ewf_handle = ewf_handle
    super(ewf_Img_Info, self).__init__(
        url="", type=pytsk3.TSK_IMG_TYPE_EXTERNAL)

  def close(self):
    self._ewf_handle.close()

  def read(self, offset, size):
    self._ewf_handle.seek(offset)
    return self._ewf_handle.read(size)

  def get_size(self):
    return self._ewf_handle.get_media_size()


class vhdi_Img_Info(pytsk3.Img_Info):
  def __init__(self, vhdi_file):
    self._vhdi_file = vhdi_file
    super(vhdi_Img_Info, self).__init__(
        url='', type=pytsk3.TSK_IMG_TYPE_EXTERNAL)

  def close(self):
    self._vhdi_file.close()

  def read(self, offset, size):
    self._vhdi_file.seek(offset)
    return self._vhdi_file.read(size)

  def get_size(self):
    return self._vhdi_file.get_media_size()


class ThreadedClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.q_m_alive = True
        self.q_f_alive = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            server, address = self.sock.accept()
            #server.settimeout(60)   # This could be a problem. We will be asyncronous and it could be hours between file requests.
            threading.Thread(target = self.listenToClient,args = (server,address)).start()

    def threaded_listener(self):
        t_listener = Thread(target=self.listen)
        t_listener.setDaemon(True)
        t_listener.start()
        print 'Listener thread started'

    def listenToClient(self, server, address):
        size = 4096
        while 1:
            try:
                data = server.recv(size)
                data_loaded = json.loads(data)
                if data:
                    if type(data_loaded) is not dict:
                        response = 'Data should be json.dumps(dict)'
                        server.send(response)
                        print response + ' -- Received: ' + str(type(data_loaded))
                    else:
                        cmd = str(data_loaded.get('cmd'))

                        if cmd =='send_dedup_path':
                            global  image_data_dedup_path
                            image_data_dedup_path = data_loaded.get("dedup_path")
                            response = "get dedup path"
                            server.send(response)

                        elif cmd == 'port from file receiver':
							print 'got port from file reeceiver'
							port_file_receiver = data_loaded.get("port_file_thread")
							print 'port_file', port_file_receiver
							response = 'got file receiver port' + str(port_file_receiver)
							server.send(response)
							try:
								file_worker_thread(port_file_receiver)
							except:
								print 'file worker thread exception'


                else:
                    raise NameError('Client disconnected')
            except:
                server.close()
                return False

# -- FUNCTIONS --

def file_worker_thread(port):
	q_f_worker = Thread(target=q_file_worker, args=(q_f, port))
	q_f_worker.setDaemon(True)
	q_f_worker.start()
	print '\tq_file worker created - ', q_f_worker.name, port


def q_file_worker(q_f, port):    #This is giving trouble... consider sending files as Hannah did via SSH and then letting the server know they are there.

    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    serverSocket.bind((ip_client, 0))
    serverSocket.connect((ip_server, port))

    while 1:
        q_f_read = q_f.get()
        q_read_string = json.loads(q_f_read)

        try:

            filepath = q_read_string.get('file_requested')  # If there is a problem cancel... improve this.
            file_fragments = q_read_string.get('file_fragments')
            filepath = filepath.replace('SPACE', ' ')
            correct_partition_offset = q_read_string.get('files_partition_offset')
            inode = q_read_string.get('inode')

            if "vhd" in args.imagefile:
                vhdi_file = pyvhdi.file()
                vhdi_file.open(args.imagefile)
                img_info = vhdi_Img_Info(vhdi_file)
                filesystemObject = pytsk3.FS_Info(img_info, offset=(correct_partition_offset))  # we should get this from the file request. There is no reason for all files to be in the same partition.
            else:
                imagehandle = pytsk3.Img_Info(args.imagefile)
                filesystemObject = pytsk3.FS_Info(imagehandle, offset=current_partition_offset)  # we should get this from the file request. There is no reason for all files to be in the same partition.

            f = filesystemObject.open_meta(inode=inode)

            fileobject = filesystemObject.open(filepath)

            file_size = fileobject.info.meta.size

            file_name = fileobject.info.name.name

            print "\t\t\t\t\tTRYING TO SEND " + filepath.encode('utf-8').strip() + ' (' + str(file_size) + ' bytes)'

            send_msg(serverSocket, q_f_read)
            received = recv_msg(serverSocket)

            if received == "Ok, send file now":
                try:
                    print 'Started to sending... ' + filepath + ' to Server at port', port
                    l = fileobject.read_random(0, file_size)

                    global noOfFilesSent
                    noOfFilesSent += 1

                    global transmittedFileSize
                    transmittedFileSize += file_size
                    send_msg(serverSocket, l)

                    #server check the file data right or not, if hash not match, require send it again
                    t = 0
                    while True:
                        if t > 2:
                            print "Fail Sending " + filepath + ", dd now~~"
                            if file_fragments != "unknown":
                                for f_f in file_fragments:
                                    start_block = f_f[0]
                                    block_lenth = f_f[1] - start_block
                                    dd_extract_file(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path)
                            break
                        file_match_check = recv_msg(serverSocket)
                        if file_match_check == "file hash match":
                            print "Done Sending " + filepath
                            break
                        else:
                            send_msg(serverSocket, l)
                        t += 1
                    # response = recv_msg(serverSocket)
                    # print response

                except Exception,e:
                    print filepath, 'Error while sending file: \"' + str(e) + '\" in partition at offset ' + str(correct_partition_offset)
                finally:
                    if not new_TC.q_f_alive:
                        print 'killing the file sender'
                        serverSocket.sendall('No more files will be sent. Killing the file sender')
                        break

            # print recv_msg(serverSocket)

        except Exception, e:
            print filepath, 'Error while negotiating the sending of the file. Error: \"' + str(e) + '\" in partition at offset ' + str(correct_partition_offset)

        q_f.task_done()

        timer_end = time.time()
        time_taken = timer_end - timer_start
        print('File Sending finished in: ', time_taken, 'seconds')

    print 'Terminating q_file_worker ', port
    serverSocket.shutdown(socket.SHUT_WR)
    serverSocket.close()

def get_a_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ip_client = socket.gethostbyname(socket.gethostname())
    s.bind((ip_client, 0))
    # print s.getsockname()[1]
    free_port = s.getsockname()[1]
    return free_port


def q_metadata_worker(q_m, ip_server, port_server, i):


    try:
        sock_c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # Socket is not thread safe... so we work with a new one for each thread. Another option would be to use a single one and a lock.
        sock_c.connect((ip_server, port_server))
    except Exception,e:
        print str(e)

    #step 1: get a free port for client and send this to server through main thread
    metadata_port = get_a_free_port()

    sock_m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # Socket is not thread safe... so we work with a new one for each thread. Another option would be to use a single one and a lock.
    sock_m.bind((ip_client, metadata_port))  # Bind to the port
    sock_m.listen(5)  # Now wait for connection.

    message = {"cmd": 'new client q_metadata_worker',
               "worker_num": i,
               "metadata_port": metadata_port}
    response = send_serialized_message(sock_c, message)
    sock_c.close()

    clientSocket, addr = sock_m.accept()  # Establish connection with client.   See about reusing the same socket (problem with message order)
    file_transfer_port = clientSocket.getsockname()[1]
    print 'q_metadata_worker: Got connection from', addr, file_transfer_port
    while 1:

        q_read = q_m.get()
        q_read_string = json.dumps(q_read)

        file_fragments = q_read.get('file_fragments')
        partition_dir = q_read.get('partition_dir')
        if 'NTFS' in str(partition_dir):
            block_size = 4096
        elif 'FAT' in str(partition_dir):
            block_size = 512

        send_msg(clientSocket, q_read_string)
        received = recv_msg(clientSocket)

        if received == 'OK':
            global verbose
            if verbose:
                print '- Metadata worker ' + str(i) + ' sent metadata ' + str(q_read.get("sha1"))  # we may not always need a response... make this an option of send_serialized_message to save time.
        else:
            print received

        received = recv_msg(clientSocket)
        if received == 'q_f':
            received = recv_msg(clientSocket)
            q_f.put(received)
        if file_fragments != "unknown":
            print "extract_file_slack"
            for f_f in file_fragments:
                end_block = f_f[1]
                dd_extract_duplicated_file_slack(block_size, end_block-1, 1, full_image_path, image_data_dedup_path)

        q_m.task_done()

        if not new_TC.q_m_alive:
            break

    print 'Terminating q_metadata_worker ', i
    clientSocket.close()


def send_serialized_message(sock, data):    # Consider using encryption.
    data_string = json.dumps(data)
    try:
        sock.sendall(data_string)
        response = sock.recv(4096)
        return response
    except Exception, e:
        print 'ERROR in send_serialized_message()' + str(e)


def wait_for_server(ip_server, port_server):
    global serverConnected
    num = 0
    sock_reach = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Reaching to the server:'
    while not serverConnected:
        try:
            sock_reach.connect((ip_server, port_server))
            sock_reach.close()
            print 'The server is available.'
            serverConnected = True
            break
        except Exception, e:
            print '\tTrying to reach the server. Attempt: '+ str(num)
            num += 1

def directory_recurse(partition_dir, total_count, directoryObject, parentPath,
                      fs, partition_offset):  # consider max_file_size as a useful parameter


    for entryObject in directoryObject:
        if entryObject.info.name.name in [".", ".."]:
            continue
        try:
            f_type = entryObject.info.meta.type
        except:
            global verbose
            if verbose:
                print "Cannot retrieve type of", entryObject.info.name.name  # We should keep track of these.
            continue
        try:
            if f_type == pytsk3.TSK_FS_META_TYPE_DIR:  # If it's a directory we recurse
                sub_directory = entryObject.as_directory()
                parentPath.append(entryObject.info.name.name)
                directory_recurse(partition_dir, total_count, sub_directory, parentPath, fs, partition_offset)
                parentPath.pop(-1)

            elif f_type == pytsk3.TSK_FS_META_TYPE_REG \
                    and entryObject.info.meta.size != 0 \
                    and '._' not in entryObject.info.name.name:  # If file send info to queue

                total_count[0] += 1
                file_size = entryObject.info.meta.size

                inode = int(entryObject.info.meta.addr)
                start_blocks = []  # ... was Hanna already dealing with fragmentation?
                end_blocks = []
                frag_offset = []
                total_blocks = 0
                f = fs.open_meta(inode=inode)
                for attr in f:
                    for run in attr:
                        start_blocks.append(run.addr + partition_start)
                        end_blocks.append(run.addr + partition_start + run.len)
                        total_blocks += run.len
                        frag_offset.append(run.offset)

                if len(start_blocks) != 0:
                    start_block = start_blocks[0]  # A very select few files specify 2 block offsets, here I get the first starting block
                    end_block =  end_blocks[-1]
                    file_fragments = [list(a) for a in zip(start_blocks, end_blocks, frag_offset)] #merge 2 lists into 1 two_dimensional lists to store fragement files

                else:
                    start_block = 'unknown'  # The block offset for a select few files could not be found
                    end_block = 'unknown'
                    file_fragments = 'unknown'

                file_name = entryObject.info.name.name

                if file_name == "FNTCACHE.DAT":
                    for f_f in file_fragments:
                        start_block = f_f[0]
                        block_lenth = f_f[1] - f_f[0]
                        dd_extract_file(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path)
                if file_name == "SYSTEM":
                    for f_f in file_fragments:
                        start_block = f_f[0]
                        block_lenth = f_f[1] - f_f[0]
                        dd_extract_file(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path)
                if total_blocks > file_size/fs_bs:
                    i = 0
                    for f_f in file_fragments:
                        if i == 0:
                            i += 1
                            continue
                        else:
                            i += 1
                            start_block = f_f[0]
                            block_lenth = f_f[1] - f_f[0]
                            dd_extract_file(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path)

                extension = os.path.splitext(file_name)[1]
                extension_list = [".tmp",".sys",".PNF",".log",".lkg",".pf",".dat", ".hve", ".mui",".dll",".ddl",
                                  ".DAT",".bmp",".cab",".new", ".cdf-ms",".fx",".evtx",
                                  ".001", ".002", ".cat", ".lnk", ".manifest", ".db", ".etl",
                                  ".xml", ".wer", ".bin", ".ini"]
                if any(str(extension) in s for s in extension_list) and start_block != 'unknown':
                    print "file_name_extension", file_name
                    for f_f in file_fragments:
                        start_block = f_f[0]
                        block_lenth = f_f[1] - f_f[0]
                        dd_extract_file_by_extension(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path)

                file_path = '/%s/%s' % ('/'.join(parentPath), file_name)
                file_path = file_path.replace(' ', 'SPACE')
                captured_date = [datetime.datetime.now().strftime('%Y%m%d%H%M%S')]
                number_of_blocks = str(total_blocks)

                start_block = str(start_block)
                end_block = str(end_block)

                inode = int(entryObject.info.meta.addr)
                partition_address = partition.addr
                filedata = entryObject.read_random(0, file_size)

                hasher_worker = Thread(target=prepare_metadata, args=(
                    partition_dir, inserter_count, filedata, file_name, number_of_blocks, start_block, end_block, file_fragments,
                    inode, partition_address, partition_offset, file_path, file_size, captured_date))

                hasher_worker.setDaemon(True)
                hasher_worker.start()

                if not file_fragments == 'unknown':
                    # print 'sign the blocks allocated to the file as 1'
                    for f_f in file_fragments:
                        start_block = f_f[0]
                        end_block = f_f[1]
                        for i in range(start_block, end_block):
                            drive_block_list[i] = 1

            elif f_type == pytsk3.TSK_FS_META_TYPE_REG and entryObject.info.meta.size == 0:  # Claudio: we should do something with this.
                if verbose:
                    print "file of size 0"

        except IOError as e:
            print e, 'exception!!!'
            continue

def prepare_metadata(partition_dir, inserter_count, filedata, file_name, number_of_blocks, start_block, end_block, file_fragments, inode,
                     partition_address, partition_offset, file_path, file_size, captured_date):
    global noOfFilesOnDisk
    noOfFilesOnDisk += 1

    global diskCumulativeFileSize
    diskCumulativeFileSize += file_size

    sha1hash = hashlib.sha1()
    sha1hash.update(filedata)


    result = {
        "partition_dir": partition_dir,
        "filename": file_name,
        "acquisition_id": acquisition_id,
        "number_of_blocks": number_of_blocks,
        "start_block": start_block,  # Make this a list to represent fragmentation
        "end_block": end_block,
        # Add final block as a binary string
        "file_fragments": file_fragments,
        "inode": inode,
        "partition_address": partition_address,
        "partition_offset": partition_offset,
        "filepath": file_path,
        "size": file_size,
        "captured_date": captured_date,
        "image_name": image_name,
        "sha1": sha1hash.hexdigest(),  # Make this a list to included all hashes requested.
        "image_size": image_size
    }
    # print result

    q_m.put(result)  # send to queue as a dictionary (mongoDB ready format)
    # global verbose
    # if verbose:
    #     print 'Metadata added to q_m.\tInserter_count:', inserter_count[0]
    inserter_count[0] -= 1  # new thread available.

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

def send_client_port():

    print 'send client port'
    sock_c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # Socket is not thread safe... so we work with a new one for each thread. Another option would be to use a single one and a lock.
    sock_c.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_c.bind((ip_server, 0))
    global ip_client
    global port_client
    port_client = sock_c.getsockname()[1]
    ip_client = get_ip_address()
    sock_c.connect((ip_server, port_server))
    message = {"cmd": 'client port',
           "port_client": port_client,
           "ip_client": ip_client}
    response = send_serialized_message(sock_c, message)

    print '\tSERVER SAYS:', response
    print port_client
    print ip_client

    global serverConnected
    serverConnected = True
    sock_c.close()

def get_ip_address():
   s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   s.connect(("8.8.8.8", 80))
   ip_address = s.getsockname()[0]
   s.close()
   return ip_address


if __name__ == "__main__":

    global verbose
    verbose = False

    argparser = argparse.ArgumentParser(description='Hash files recursively from a forensic image and optionally extract them')
    argparser.add_argument(
        '-i', '--image',
        dest='imagefile',
        action="store",
        type=str,
        default=None,
        required=True,
        help='E01 to extract from'
    )

    argparser.add_argument(
        '-t', '--type',
        dest='imagetype',
        action="store",
        type=str,
        default=False,
        required=True,
        help='Specify image type e01 or raw'
    )


    args = argparser.parse_args()

    global serverConnected
    serverConnected = False

    global timer_start

    ip_server = socket.gethostbyname(socket.gethostname())
    port_server = 15554

    send_client_port()

    dir_path = ''  # We can specify path in image to be analysed. Not used when examining entire file

    full_image_path = args.imagefile
    image_name = os.path.basename(full_image_path)

    acquisition_id = args.imagefile + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')  # Consider adding more info...
    inserter_count = [0]  # Passed as a reference... watch out for thread safeness. This should be a java volatile like kind of variable.
    total_count = [0]
    # current_partition_offset = block_size    #This is the offset needed for file reading with pytsk3... this will need to be dynamic (send with metadata maybe?)..
    # but for the test its good enough

    # while not serverConnected:
    #     print "Server Not Connected"
    #     time.sleep(1)

    # wait_for_server(ip_server, port_server)  # Tries to connect to the server until it's available... maybe a timeout would be good here.

    print 'Raising the client listener thread now'
    new_TC = ThreadedClient(ip_client, port_client)
    new_TC.threaded_listener()

    #server create a dir for receive files, client wait for this dir then start transmission
    while image_data_dedup_path == '':
        time.sleep(0.5)


    print 'Preparing Metadata Queue:'
    q_m = Queue(maxsize = 0)
    q_m_num_threads = 5
    q_m_num_inserters = 5
    for i in range(q_m_num_threads):
        q_m_worker = Thread(target=q_metadata_worker, args=(q_m, ip_server, port_server, i))
        q_m_worker.setDaemon(True)
        q_m_worker.start()
        print '\tq_metadata_worker created - ', q_m_worker.name, i + 1


    print 'Preparing File Queue:'
    q_f = Queue(maxsize = 0)
    q_f_num_threads = 5

    # Run Disk Reader
    timer_start = time.time()
    print 'timer started'
    print ('\nDISK READER started.\n\tImage: ' + str(args.imagefile) + '\n\tDirectory path: ' + str(dir_path))

    # if (args.imagetype == "e01"):
    #     filenames = pyewf.glob(args.imagefile)
    #     ewf_handle = pyewf.handle()
    #     ewf_handle.open(filenames)
    #     imagehandle = ewf_Img_Info(ewf_handle)
    # elif (args.imagetype == "raw"):
    #     print "Raw Type"
    if "vhd" in args.imagefile:
        vhdi_file = pyvhdi.file()
        vhdi_file.open(args.imagefile)
        imagehandle = vhdi_Img_Info(vhdi_file)
        partitionTable = pytsk3.Volume_Info(imagehandle)
    else:
        imagehandle = pytsk3.Img_Info(args.imagefile)
        partitionTable = pytsk3.Volume_Info(imagehandle)

    print 'PARTITIONS:'
    current_partition_offset = 0

    drive_block_number = 0
    for partition in partitionTable:
        if "Primary Table" in partition.desc:
            continue
        else:
            partition_lenth = partition.len
            drive_block_number += partition_lenth

    partition_i = 0
    for partition in partitionTable:
        print '\t', partition.addr, partition.desc, "%s (%s)" % (
        partition.start, partition.start * sector_size), partition.len  # Print partition information
        partition_len = partition.len

        if 'NTFS' in partition.desc:  # Use DFIR tutorial example to deal with other partitions later on.
            partition_i += 1

            print '--- Recursing on partition:', partition.addr
            partition_name = partition.desc
            partition_name = partition_name.replace("/", "")
            partition_dir = partition_name + str(partition_i)
            if partition_i == 1:
                drive_block_list = [0] * (drive_block_number/8)
            fs_bs = 4096
            ss_bs = 1
            partition_start = partition.start/8
            current_partition_offset = partition.start * 512
            filesystem_object = pytsk3.FS_Info(imagehandle, offset=(current_partition_offset))
            block_size = filesystem_object.info.block_size
            directory_object = filesystem_object.open_dir(path = dir_path)
            image_size = pytsk3.Img_Info.get_size(imagehandle)
            directory_recurse(partition_dir, total_count, directory_object, [], filesystem_object, current_partition_offset)

            partition_sha1 = generate_part_sha1(full_image_path, current_partition_offset*sector_size)
            partition_size = partition.len * sector_size
            print 'partition_sha1', partition_sha1
        elif 'FAT32' in partition.desc:  # Use DFIR tutorial example to deal with other partitions later on.
            partition_i += 1
            print '--- Recursing on partition:', partition.addr
            partition_name = partition.desc
            partition_dir = partition_name + str(partition_i)

            try:
                partition_start = partition.start
                fs_bs = 512
                ss_bs = 8
                drive_block_list = [0] * drive_block_number
                current_partition_offset = partition.start * 512
                filesystem_object = pytsk3.FS_Info(imagehandle, offset=(current_partition_offset))
                block_size = filesystem_object.info.block_size
                directory_object = filesystem_object.open_dir(path = dir_path)
                image_size = pytsk3.Img_Info.get_size(imagehandle)

                directory_recurse(partition_dir, total_count, directory_object, [], filesystem_object, current_partition_offset)
            except Exception,e:
                print str(e)

            partition_sha1 = generate_part_sha1(full_image_path, current_partition_offset*sector_size)
            partition_size = partition.len * sector_size

            print 'partition_sha1', partition_sha1

    print ('-- READING FINISHED --')

    unallocated_block_list = get_continuous_unallocated_block(drive_block_list)
    print 'unallocated_block_list', unallocated_block_list

    # unallocated_dir = create_unallocated_dir(acquisition_id)
    for i in range(len(unallocated_block_list)):
        start = unallocated_block_list[i][0]
        end = unallocated_block_list[i][1]
        extract_data_worker = Thread(target=extract_unallocated_data, args=(full_image_path, start, end, fs_bs, image_data_dedup_path))
        extract_data_worker.setDaemon(True)
        extract_data_worker.start()


    timer_end = time.time()
    time_taken = timer_end - timer_start
    print ('Reading finished at: ', time_taken, 'seconds')

    # TODO: new thread... Hash the whole partition and give it to the server.


    global sock_main
    sock_main = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_main.connect((ip_server, port_server))

    image_hash = generate_image_sha1(full_image_path)
    print image_hash
    print 'image_hash'
    time.sleep(5)

    message = {"cmd": 'sending_image_info', "image_id": acquisition_id, "image_name": args.imagefile,
               "acquisition_time": timer_start, "image_size": image_size, "image_hash": image_hash,
               "partition_sha1": partition_sha1, "partition_size": partition_size, "partition_name": partition_name}
    response = send_serialized_message(sock_main, message)
    print response

    q_m.join()  # Wait for depleters to finish
    while not q_m.empty():
        time.sleep(5)
    print '\t\t\t\tMETADATA FINISHED BEING SENT TO SERVER'
    new_TC.q_m_alive = False
    message = {"cmd": 'Done sending metadata'}
    response = send_serialized_message(sock_main, message)  # Let server know reading is done
    print response

    q_f.join()  # Wait for files to be sent
    t = 0
    while not q_f.empty():
        time.sleep(5)
        t += 5
        if t > 500:
            print q_f
            # break
    print '\t\t\t\tFILES FINISHED BEING SENT TO SERVER'
    new_TC.q_f_alive = False
    message = {"cmd": 'Done sending files'}
    response = send_serialized_message(sock_main, message) # Let the server know the file sending is finished
    print response

    timer_end = time.time()
    time_taken = timer_end - timer_start
    print ('Transmission finished at: ', time_taken, 'seconds')

    message = {"cmd": 'SHUTDOWN'}
    response = send_serialized_message(sock_main, message)
    print response
    sock_main.close()

    print '-- CLIENT SIDE HAS FINISHED --'

    # Log Folder
    logDirectory = "logs"
    try:
        os.mkdir(logDirectory)
    except Exception:
        pass

    logTimeStamp = str((datetime.datetime.now())).replace(':', '.')

    print args.imagefile + ',' + str(time_taken) + ', ' + str(noOfFilesOnDisk) + ', ' + str(diskCumulativeFileSize) \
          + ', ' + str(noOfFilesSent) + ', ' + str(transmittedFileSize) + ', ' + str((transmittedFileSize / 1024 / 1024) / time_taken) + ', ' \
          + str(diskCumulativeFileSize / 1024 / 1024 / time_taken)

    running_log_file = logDirectory + os.sep + 'data collected ' + '.csv'

    if not os.path.exists(running_log_file):
        csv_file = open(running_log_file, mode = 'ab')
        wr = csv.writer(csv_file)
        wr.writerow(
            ['Log Time Stamp', 'Image Processed', 'Reading Time (s)', 'Files on Disk', 'File Size on Disk', 'Files Sent to Server', 'Total Bytes Sent to Server',
            'Actual System Thoughput (MB/sec)', '"Effective" System Throughput (MB/sec)', 'duplication rate'])
        wr.writerow([logTimeStamp ,args.imagefile , str(time_taken),str(noOfFilesOnDisk) , str(diskCumulativeFileSize)
        , str(noOfFilesSent) ,str(transmittedFileSize) , str((transmittedFileSize / 1024 / 1024) / time_taken),
             str(diskCumulativeFileSize / 1024 / 1024 / time_taken), str((diskCumulativeFileSize - transmittedFileSize)/float(diskCumulativeFileSize))])
        csv_file.close()
    else:
        try:

            with open(running_log_file, mode = 'ab') as csv_file:
                wr = csv.writer(csv_file)
                wr.writerow([logTimeStamp ,args.imagefile , str(time_taken),str(noOfFilesOnDisk) , str(diskCumulativeFileSize)
                        , str(noOfFilesSent) ,str(transmittedFileSize) , str((transmittedFileSize / 1024 / 1024) / time_taken),
                             str(diskCumulativeFileSize / 1024 / 1024 / time_taken), str((diskCumulativeFileSize - transmittedFileSize)/float(diskCumulativeFileSize))])
                csv_file.close()
        except ZeroDivisionError as e:
            print e
    print 'Client STOPS now. TIME--', time_taken/3600/3600

