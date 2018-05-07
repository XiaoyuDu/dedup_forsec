'''

'''


import os
import pytsk3
import subprocess
from operator import itemgetter
from itertools import groupby
import ntpath
import time


def get_slack(f, partition_name):
    """ Returns the slack space of the block. """

    # Walk all blocks allocated by this file as in some filesystems
    # each file has several attributes which can allocate multiple
    # blocks.

    l_offset = 0
    data = 'None'

    for attr in f:
        for run in attr:
            l_offset = run.len

    if "NTFS" in partition_name:
        l_block = (l_offset - 1) * 4096

    elif "FAT" in partition_name:
        l_block = (l_offset - 8) * 512


    # Last block of the file
    size = f.info.meta.size
    l_d_size = size % 4096

    # Slack space size
    s_size = 4096 - l_d_size

    # Force reading the slack of the file providing the FLAG_SLACK
    try:
        data = f.read_random(l_block + l_d_size, s_size, pytsk3.TSK_FS_ATTR_TYPE_DEFAULT, 0,
                             pytsk3.TSK_FS_FILE_READ_FLAG_SLACK)
    except Exception, e:
        # print file_name, 'get_slack-read_random'
        print e

    return data, s_size


def get_continuous_unallocated_block(drive_block_list):
    unallocated_block = []
    for i in range(len(drive_block_list)):
        if drive_block_list[i] == 0:
            unallocated_block.append(i)

    continuous_unallocated_blocks = []
    for k, g in groupby(enumerate(unallocated_block), lambda (i, x): i - x):
        group = map(itemgetter(1), g)
        continuous_unallocated_blocks.append((group[0], group[-1]))

    return continuous_unallocated_blocks



def extract_unallocated_data(full_image_path, start, end, fs_bs, image_data_dedup_path):
    print start, end
    directory = image_data_dedup_path + os.sep + 'unallocated_space_data'
    print directory
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception:
        pass

    input_data = full_image_path
    count = end - start + 1
    out_put = directory + os.sep + str(start) + '_' + str(end) + '_' + str(count)
    subprocess.Popen(args=['./read_unallocated_data.sh', '%s' % str(input_data), '%s' % str(out_put),
            '%s' % str(start), '%s' % str(count), '%s' % str(fs_bs)])

