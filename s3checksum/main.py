import argparse
from pathlib import Path
from . import etag


def pathlib_exists(str_path: str) -> Path:
    path = Path(str_path)
    if not path.exists():
        raise argparse.ArgumentTypeError(f'Path {str_path} does not exist!')
    return path


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser('Calculates and compares S3 etags')
    sub = parser.add_subparsers(dest='command')

    local = sub.add_parser('local')
    local.add_argument('--path', type=pathlib_exists, required=True)

    remote = sub.add_parser('remote')
    remote.add_argument('--bucket', required=True)
    remote.add_argument('--key', required=True)
    remote.add_argument('--endpoint', default=None)

    return parser


def entry(args):
    if args.command == 'remote':
        print(etag.etag_remote(args.bucket, args.key, args.endpoint))
    elif args.command == 'local':
        print(etag.etag_local(args.path))


def main():
    args = get_parser().parse_args()
    entry(args)


if __name__ == '__main__':
    main()
