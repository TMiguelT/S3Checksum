from pathlib import Path
import hashlib
import boto3
import botocore
import math
from typing import NamedTuple, Generator, Optional

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


def etag_remote(bucket: str, key: str, endpoint: str = None) -> Optional[str]:
    """
    Returns the S3 etag for a file in S3
    :param bucket: The S3 bucket the file is contained within
    :param key: The path in the S3 bucket to the file of interest
    :return: The etag of interest, or None, if the file doesn't exist
    """
    s3 = boto3.client('s3', endpoint_url=endpoint)
    try:
        return s3.head_object(
            Bucket=bucket,
            Key=key
        )['ETag'].replace('"', '')
    except botocore.exceptions.ClientError:
        return None


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


def compare_file(local_path: Path, remote_bucket: str, bucket_key: str, ignore_symlinks=False,
                 remote_endpoint: str = None, chunksize: int = DEFAULT_CHUNKSIZE) -> Generator[
    EtagComparison, None, None]:
    """
    Compare the etags of a local and remote file or directory
    :return: Yields an iterable of EtagComparison objects, representing the comparison between all files of interest
    """
    if local_path.is_dir():
        # If the path points to a directory, recursively compare
        for subpath in local_path.rglob('*'):

            if subpath.is_symlink() and ignore_symlinks:
                # Skip symlinks if requested
                continue

            if subpath.is_file():
                yield from compare_file(
                    local_path=subpath,
                    remote_bucket=remote_bucket,
                    bucket_key=bucket_key + '/' + str(subpath.relative_to(local_path)),
                    ignore_symlinks=ignore_symlinks,
                    remote_endpoint=remote_endpoint,
                    chunksize=chunksize
                )

    else:
        # If the path points to a file, just do a single comparison

        # First, download the hash of the remote file, and use this to determine if we're chunking
        remote = etag_remote(
            remote_bucket,
            bucket_key,
            endpoint=remote_endpoint
        )

        # Next, hash the local file, chunking if the remote file was also chunked
        local = etag_local(
            local_path,
            chunksize=chunksize,
            multipart_threshold=0 if remote is not None and '-' in remote else math.inf
        )

        yield EtagComparison(
            remote=remote,
            local=local,
            filename=str(local_path)
        )
