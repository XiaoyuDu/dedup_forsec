

import argparse

import hashlib
from pymongo import MongoClient
import subprocess
import unicodedata
import time
import os
from drive_image import generate_image_sha1

global block_size
block_size = 4096

def special_data(dedup_root_path, special_data_dir, target, fs_bs, total_insert_blocks):
    special_file_extension_dir = dedup_root_path + os.sep + special_data_dir
    i = 0
    if os.path.exists(special_file_extension_dir):
        for special_file_extension_data in os.listdir(special_file_extension_dir):
            i += 1
            unallocated_block_list = [int(s) for s in special_file_extension_data.split('_') if s.isdigit()]
            special_file_blocks_file = special_file_extension_dir + os.sep + special_file_extension_data
            seek = unallocated_block_list[0]
            count = unallocated_block_list[1]
            subprocess.Popen(args=['./File_Insert.sh', '%s' % str(special_file_blocks_file), '%s' % str(target),
                                   '%s' % str(0),'%s' % str(seek), '%s' % str(count),'%s' % str(fs_bs)])
            total_insert_blocks += count
            print i, "--", special_file_blocks_file


def construct(acquisition_id):
    name = None
    memory = 0.0
    original_hash = None
    client = MongoClient()

    # 0) acquire the metadata from database
    db = client['DF_Project_1104']
    metadata_acquisition = db.metadata_acquisition
    server_files = db.server_files
    image_acquisition = db.image_acquisition
    reconstruct_image = image_acquisition.find_one({"image_id": acquisition_id})
    memory = int(reconstruct_image['image_size'])
    print "memory: ", memory
    original_hash = reconstruct_image['image_hash']

    dedup_root_path = reconstruct_image['dedup_root_path']
    image_name = reconstruct_image['image_name']
    filename, file_extension = os.path.splitext(str(image_name))

    print "original hash: ", original_hash
    memory /= 512
    name = str(acquisition_id)
    partition_name = reconstruct_image['partition_name']
    print 'partition_name', partition_name
    if 'NTFS' in partition_name:
        fs_bs = 4096
        ss_bs = 1
    elif 'FAT' in partition_name:
        fs_bs = 512
        ss_bs = 8

    print 'fs_bs, ss_bs',fs_bs, ss_bs
    # 1) - create the target blank image (write with 0)
    target = name + str(file_extension)
    subprocess.Popen(args=['./File_Insert.sh', '%s' % str("/dev/zero"), '%s' % str(target),
                           '%s' % str(memory - 1)])

    number_of_file_insert = 0
    total_insert_blocks = 0
    # total_slack_size = 0
    # slack_no = 0
    for obj in metadata_acquisition.find({"acquisition_id": acquisition_id}):

        print '-------------------------------------------------'
        file_name = obj['filename']
        fragment_files = obj['file_fragments']
        sha1 = str(obj['sha1'])
        file_size = int(obj['size'])
        # slack_size = int(obj['slack_size'])

        server_file = server_files.find_one({"sha1": sha1})
        # print 'sha1:', sha1,';size',file_size,';slack_size:', slack_size
        if server_file is not None:
            dedup_path = str(unicodedata.normalize('NFKD', server_file['DedupedPath']).encode('ascii', 'ignore'))

            print 'file:', dedup_path, 'inserted', number_of_file_insert


            if file_name == "$MFT":
                #$MFT
                start_block = fragment_files[0][0]
                end_block = fragment_files[0][1]
                skip = fragment_files[0][2]
                number_of_blocks = end_block - start_block

                seek = start_block
                count = number_of_blocks
                subprocess.Popen(args=['./File_Insert.sh', dedup_path,
                                       '%s' % str(target), '%s' % str(skip),
                                       '%s' % str(seek), '%s' % str(count), '%s' % str(fs_bs)])
                print "$MFT!!!"
                continue
            if file_name == "$Tops":
            #     #$tOPS
                print "$Tops!!!"
                continue

            if file_name == "$Boot":
                start_block = fragment_files[0][0]
                end_block = fragment_files[0][1]
                skip = fragment_files[0][2]
                number_of_blocks = end_block - start_block

                seek = start_block
                count = number_of_blocks
                subprocess.Popen(args=['./File_Insert.sh', dedup_path,
                                       '%s' % str(target), '%s' % str(skip),
                                       '%s' % str(seek), '%s' % str(count), '%s' % str(fs_bs)])

                continue
            # number_of_fragments = len(fragment_files)

            # 2) - write file data into the image
            # before the last fragment of the file, copy all data
            # for the last fragment, the last block might have slack space.

            if fragment_files != 'unknown':
                # iterator each fragment of one file and writing to target image
                number_of_file_insert += 1

                for index in range(len(fragment_files)):
                    if file_size <= 0:
                        continue

                    # print "number_of_fragments", number_of_fragments
                    start_block = fragment_files[index][0]
                    end_block = fragment_files[index][1]
                    skip = fragment_files[index][2]
                    number_of_blocks = end_block - start_block
                    if str(start_block) == "25856":
                        continue
                        print "25856"

                    seek = start_block
                    count = number_of_blocks
                    subprocess.Popen(args=['./File_Insert.sh', dedup_path,
                                           '%s' % str(target),'%s' % str(skip),
                                           '%s' % str(seek), '%s' % str(count), '%s' % str(fs_bs)])
                    total_insert_blocks += count
                    file_size -= count*fs_bs
                    print "index", index ,"start_block", start_block, "count", count, "end_block", end_block



    # 4) - write unallocated data into the image.

    # unallocated_space_dir = '/mnt/md0/code_23062017/unallocated_space_data' + os.sep + acquisition_id
    unallocated_space_dir = dedup_root_path + os.sep + 'unallocated_space_data'
    for unallocated_space_data in os.listdir(unallocated_space_dir):
        unallocated_block_list = [int(s) for s in unallocated_space_data.split('_') if s.isdigit()]
        unallocated_space_file = unallocated_space_dir + os.sep + unallocated_space_data
        seek = unallocated_block_list[0]
        count = unallocated_block_list[2]
        subprocess.Popen(args=['./File_Insert.sh', '%s' % str(unallocated_space_file), '%s' % str(target),
                               '%s' % str(0),'%s' % str(seek), '%s' % str(count),'%s' % str(fs_bs)])
        total_insert_blocks += count


    special_data_list = ['special_file_blocks', 'extract_file_by_extension', 'duplicated_file_slack']
    for special_data_dir in special_data_list:
        print special_data_dir
        special_data(dedup_root_path, special_data_dir, target, fs_bs, total_insert_blocks)

    print total_insert_blocks, 'total_insert_blocks'
    print target
    reconstructed_image_hash = generate_image_sha1(target)
    print 'original hash is ', original_hash
    print 'reconstructed image hash is ', (reconstructed_image_hash)

    if reconstructed_image_hash == original_hash:
        print "Success. Matching Hash"
    else:
        print "Fail. Hashes Do Not Match"


argparser = argparse.ArgumentParser(
    description='Reconstruct a previously acquired disk from a database')
argparser.add_argument(
    '-a', '--acquisition_id',
    dest='imagefile',
    action="store",
    type=str,
    default=None,
    required=True,
    help='Disk image to reconstruct'
)
args = argparser.parse_args()
construct(args.imagefile)
