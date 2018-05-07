import subprocess
import os

def dd_extract_file( fs_bs,start_block, block_lenth, full_image_path, image_data_dedup_path):

    print "dd_extract_file"

    out_put_path = image_data_dedup_path + os.sep + "special_file_blocks"

    try:
        if not os.path.exists(out_put_path):
            os.makedirs(out_put_path)
    except Exception:
        pass

    out_put_file = out_put_path + os.sep + str(start_block) + "_" + str(block_lenth)

    subprocess.Popen(args=['./read_unallocated_data.sh', '%s' % str(full_image_path), '%s' % str(out_put_file),
                           '%s' % str(start_block), '%s' % str(block_lenth), '%s' % str(fs_bs)]).wait()

def dd_extract_file_by_extension(fs_bs, start_block, block_lenth, full_image_path, image_data_dedup_path):

    print "dd_extract_file_by_extension"

    out_put_path = image_data_dedup_path + os.sep + "extract_file_by_extension"

    try:
        if not os.path.exists(out_put_path):
            os.makedirs(out_put_path)
    except Exception:
        pass

    out_put_file = out_put_path + os.sep + str(start_block) + "_" + str(block_lenth)

    subprocess.Popen(args=['./read_unallocated_data.sh', '%s' % str(full_image_path), '%s' % str(out_put_file),
                           '%s' % str(start_block), '%s' % str(block_lenth), '%s' % str(fs_bs)]).wait()


def dd_extract_duplicated_file_slack(block_size, start_block, block_lenth, full_image_path, image_data_dedup_path):

    print "dd_extract_duplicated_file_slack"

    out_put_path = image_data_dedup_path + os.sep + "duplicated_file_slack"

    try:
        if not os.path.exists(out_put_path):
            os.makedirs(out_put_path)
    except Exception:
        pass

    out_put_file = out_put_path + os.sep + str(start_block) + "_" + str(block_lenth)

    subprocess.Popen(args=['./read_unallocated_data.sh', '%s' % str(full_image_path), '%s' % str(out_put_file),
                           '%s' % str(start_block), '%s' % str(block_lenth), '%s' % str(block_size)]).wait()

