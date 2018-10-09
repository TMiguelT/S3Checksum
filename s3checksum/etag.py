from pathlib import Path
import hashlib
import binascii
import boto3


def hash_chunk(chunk: str):
    hash = hashlib.md5(chunk)
    return binascii.unhexlify(hash.hexdigest())


def etag_local(source_path: Path, multipart_chunksize: int = 8 * 1024 * 1024,
               multipart_threshold: int = 8 * 1024 * 1024) -> str:
    """
    Calculates the etag for a local file
    :param source_path: The file to calculate an etag for
    :param multipart_chunksize: The size of each chunk in a multipart S3 upload
    :param multipart_threshold: The size a file must be before it is chunked
    :return: An S3 etag
    """

    filesize = source_path.stat().st_size

    if filesize > multipart_threshold:
        # If the file is larger than the chunk size, hash each chunk
        multipart_hashes = []
        with source_path.open('rb') as f:
            for block in iter(lambda: f.read(multipart_chunksize), b''):
                multipart_hashes.append(hash_chunk(block))

        return hash_chunk(''.join(multipart_hashes)) + "-" + len(multipart_hashes)

    else:
        # Otherwise, hash the whole thing in one go
        hash = hashlib.md5()
        with source_path.open('rb') as f:
            for block in iter(lambda: f.read(multipart_chunksize), b''):
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
