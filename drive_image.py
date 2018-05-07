import hashlib

def generate_image_sha1(filename):
    sha1 = hashlib.sha1()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), b''): 
            sha1.update(chunk)
    
    return sha1.hexdigest()


def generate_part_sha1(filename, start):
    sha1 = hashlib.sha1()
    with open(filename, 'rb') as f:
        f.seek(start)
        for chunk in iter(lambda: f.read(8192), b''):
            sha1.update(chunk)

    return sha1.hexdigest()

