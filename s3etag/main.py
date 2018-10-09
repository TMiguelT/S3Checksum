import argparse
from pathlib import Path
from . import etag
import sys
import humanfriendly
import csv


def pathlib_exists(str_path: str) -> Path:
    """
    Argparse "type" for a file that must exist
    """
    path = Path(str_path)
    if not path.exists():
        raise argparse.ArgumentTypeError(f'Path {str_path} does not exist!')
    return path


def human_filesize(size: str) -> int:
    """
    Argparse "type" for a human-readable filesize, e.g. "15mb"
    """
    return humanfriendly.parse_size(size, binary=True)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser('Calculates and compares S3 etags')
    sub = parser.add_subparsers(dest='command')

    # These parent parsers are inherited by the concrete parsers
    local_parent = argparse.ArgumentParser(add_help=False)
    local_parent.add_argument('--path', type=pathlib_exists, required=True)
    local_parent.add_argument('--chunksize', '-c', type=human_filesize, default=etag.DEFAULT_CHUNKSIZE)
    local_parent.add_argument('--multipart-threshold', '-m', type=human_filesize, default=etag.DEFAULT_THRESHOLD)

    remote_parent = argparse.ArgumentParser(add_help=False)
    remote_parent.add_argument('--bucket', required=True)
    remote_parent.add_argument('--key', required=True)
    remote_parent.add_argument('--endpoint', default=None)

    # These are the three concrete parsers
    remote = sub.add_parser('remote', parents=[remote_parent])
    local = sub.add_parser('local', parents=[local_parent])
    compare = sub.add_parser('compare', parents=[remote_parent, local_parent], help='')
    compare.add_argument('--verbose', '-v', action='store_true', default=None,
                         help='Print all comparisons, even when successful')

    return parser


def entry(args):
    """
    Main function for the CLI
    """
    if args.command == 'remote':
        print(etag.etag_remote(args.bucket, args.key, args.endpoint))
    elif args.command == 'local':
        print(etag.etag_local(args.path, args.chunksize, args.multipart_threshold))
    elif args.command == 'compare':
        any_errors = False
        writer = csv.writer(sys.stdout, delimiter='\t')
        writer.writerow(['filepath', 'local_hash', 'remote_hash', 'equal'])

        for comparison in etag.compare_file(local_args=dict(
                path=args.path,
                chunksize=args.chunksize,
                multipart_threshold=args.multipart_threshold
        ), remote_args=dict(
            bucket=args.bucket,
            key=args.key,
            endpoint=args.endpoint
        )):

            if comparison.equal:
                # If the hashes are fine, only report it if we're in verbose mode
                if args.verbose:
                    writer.writerow([comparison.filename, comparison.local, comparison.remote, comparison.equal])
            else:
                any_errors = True
                writer.writerow([comparison.filename, comparison.local, comparison.remote, comparison.equal])

        # If any hashes were incorrect, exit was a failure code
        if any_errors:
            sys.exit(1)
        else:
            sys.exit(0)


def main():
    """
    Command-line entrypoint
    """
    args = get_parser().parse_args()
    entry(args)


if __name__ == '__main__':
    main()
