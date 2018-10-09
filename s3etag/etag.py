from pathlib import Path
import hashlib
import boto3
from typing import NamedTuple, List, Generator

DEFAULT_CHUNKSIZE = 8 * 1024 * 1024
DEFAULT_THRESHOLD = 8 * 1024 * 1024


def hash_chunk(chunk: str, hex=True):
    h = hashlib.md5(chunk)
    if hex:
        return h.hexdigest()
    else:
        return h.digest()


def etag_local(path: Path, chunksize: int = DEFAULT_CHUNKSIZE,
               multipart_threshold: int = DEFAULT_THRESHOLD) -> str:
    """
    Calculates the etag for a local file
    :param path: The file to calculate an etag for
    :param chunksize: The size of each chunk in a multipart S3 upload
    :param multipart_threshold: The size a file must be before it is chunked
    :return: An S3 etag
    """

    filesize = path.stat().st_size

    if filesize > multipart_threshold:
        # If the file is larger than the chunk size, hash each chunk
        multipart_hashes = []
        with path.open('rb') as f:
            for block in iter(lambda: f.read(chunksize), b''):
                multipart_hashes.append(hash_chunk(block, hex=False))

        return '{}-{}'.format(hash_chunk(b''.join(multipart_hashes)), len(multipart_hashes))

    else:
        # Otherwise, hash the whole thing in one go
        hash = hashlib.md5()
        with path.open('rb') as f:
            for block in iter(lambda: f.read(chunksize), b''):
                hash.update(block)
        return hash.hexdigest()


def etag_remote(bucket: str, key: str, endpoint: str = None) -> str:
    """
    Returns the S3 etag for a file in S3
    :param bucket: The S3 bucket the file is contained within
    :param key: The path in the S3 bucket to the file of interest
    :return: The etag of interest
    """
    s3 = boto3.client('s3', endpoint_url=endpoint)
    return s3.head_object(
        Bucket=bucket,
        Key=key
    )['ETag'].replace('"', '')


class EtagComparison(NamedTuple):
    """
    Class containing comparison data between a remote and local etag
    """
    remote: str
    local: str
    filename: str

    @property
    def equal(self) -> bool:
        """
        True if the remote and local etags were equal
        """
        return self.local == self.remote


def compare_file(local_args, remote_args) -> Generator[EtagComparison, None, None]:
    """
    Compare the etags of a local and remote file or directory
    :param local_args:
    :param remote_args:
    :return: Yields an iterable of EtagComparison objects, representing the comparison between all files of interest
    """
    local_path: Path = local_args['path']

    if local_path.is_dir():
        # If the path points to a directory, recursively compare
        for subpath in local_path.rglob('*'):
            if subpath.is_file():
                subpath_localargs = dict(**local_args)
                subpath_localargs['path'] = subpath

                subpath_remoteargs = dict(**remote_args)
                subpath_remoteargs['key'] = subpath_remoteargs['key'] + '/' + str(subpath.relative_to(local_path))

                yield from compare_file(subpath_localargs, subpath_remoteargs)

    else:
        # If the path points to a file, just do a single comparison
        yield EtagComparison(
            remote=etag_remote(**remote_args),
            local=etag_local(**local_args),
            filename=str(local_path)
        )
