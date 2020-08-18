"""
Atomic operations on a network-file-system (NFS)
------------------------------------------------

Network-file-systems (really any flavor of these) have high latencies.
To avoid that files are copied or written only partly when a
process dies, this module attempts to use temporary-files first, and then move
the temporary-files atomically to the desired path in the last moment.

The benefit is, that you will notice when your files are incomplete, because
then they do not exist with the name that you are looking for. Instead there is
only a broken file with a similar, but randomized name in the same directory.
"""
import uuid
import os
import shutil
import errno


def copy(src, dst):
    copy_id = uuid.uuid4().__str__()
    tmp_dst = "{:s}.{:s}.tmp".format(dst, copy_id)
    try:
        shutil.copytree(src, tmp_dst)
    except OSError as exc:
        if exc.errno == errno.ENOTDIR:
            shutil.copy2(src, tmp_dst)
        else:
            raise
    os.rename(tmp_dst, dst)


def move(src, dst):
    try:
        os.rename(src, dst)
    except OSError as err:
        if err.errno == errno.EXDEV:
            copy(src, dst)
            os.unlink(src)
        else:
            raise


def write(content, path, mode="wt"):
    """
    Writes entire content to temporary-file near path and attempts an atomic
    move to the final path.
    """
    copy_id = uuid.uuid4().__str__()
    tmp_path = "{:s}.{:s}.tmp".format(path, copy_id)
    with open(tmp_path, mode) as f:
        f.write(content)
    move(src=tmp_path, dst=path)


def read(path, mode="rt"):
    """
    I think reading an entire file is rather safe across the nfs.
    But in case I am wrong, here is the wrapper.
    """
    with open(path, mode) as f:
        content = f.read()
    return content
