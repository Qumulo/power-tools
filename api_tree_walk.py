import argparse
import glob
import multiprocessing

from dataclasses import dataclass
from typing import Any, BinaryIO, Mapping
from qumulo.rest_client import RestClient


MAX_DEPTH = 5
NUM_WORKERS = 16
PAGE_SIZE = 1000
REST_PORT = 8000


@dataclass
class Directory:
    path: str
    max_depth: int


class FileEntry:
    def __init__(self, entry_dict: Mapping[str, Any]):
        self.name = entry_dict['name']
        self.size = entry_dict['size']
        self.file_type = entry_dict['type']
        self.child_count = entry_dict['child_count']

    def is_directory(self) -> bool:
        return self.file_type == "FS_FILE_TYPE_DIRECTORY"

    def has_children(self) -> bool:
        return self.child_count > 0


def read_file_attributes(
    directory: Directory,
    entry: FileEntry,
    out_file: BinaryIO
) -> None:
    """
    Read any pertinent file attributes from the Entry JSON into the output file

    Here is a sample Entry, if you want to read additional attributes:
    {
        'path': '/tweets/snow/tweets-snow-2019011902.jsonline',
        'name': 'tweets-snow-2019011902.jsonline',
        'change_time': '2019-01-19T10:55:47.591487372Z',
        'creation_time': '2019-01-19T09:56:01.10147995Z',
        'modification_time': '2019-01-19T10:55:47.591487372Z',
        'child_count': 0,
        'extended_attributes': {'archive': True,
                                'compressed': False,
                                'hidden': False,
                                'not_content_indexed': False,
                                'read_only': False,
                                'system': False,
                                'temporary': False},
        'id': '11041240942',
        'group': '17179869184',
        'group_details': {'id_type': 'NFS_GID', 'id_value': '0'},
        'owner': '12884901888',
        'owner_details': {'id_type': 'NFS_UID', 'id_value': '0'},
        'mode': '0644',
        'num_links': 1,
        'size': '34342740',
        'symlink_target_type': 'FS_FILE_TYPE_UNKNOWN',
        'type': 'FS_FILE_TYPE_FILE'
    }
    """
    out_file.write(
        f"{directory.path}{entry.name}\t{entry.size}\t{entry.file_type}\n".encode('utf-8')
    )


def read_all_file_attributes_in_dir(
    rest_client: RestClient,
    directory: Directory,
    out_file: BinaryIO,
    worker_queue: multiprocessing.JoinableQueue
) -> None:
    """
    Write attributes from each file in the given directory, recursively, to the
    out_file
    """
    response = rest_client.fs.read_directory(
            path=directory.path, page_size=PAGE_SIZE)

    while response:
        for entry_dict in response['files']:
            entry = FileEntry(entry_dict)
            read_file_attributes(directory, entry, out_file)

            if entry.is_directory() and entry.has_children():
                next_dir = Directory(
                        f"{directory.path}{entry.name}/", directory.max_depth)
                worker_queue.put(next_dir)

        if 'paging' in response and 'next' in response['paging']:
            response = rest_client.request("GET", response['paging']['next'])


def worker_main(
    rest_client: RestClient,
    worker_queue: multiprocessing.JoinableQueue
) -> None:
    proc = multiprocessing.current_process()

    with open(f"out-{proc.pid}.txt", "wb") as out_file:
        while True:
            directory = worker_queue.get(block=True)
            read_all_file_attributes_in_dir(
                    rest_client, directory, out_file, worker_queue)
            worker_queue.task_done()


class ApiTreeWalker:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        rest_client: RestClient
    ):
        self.host = host
        self.username = username
        self.password = password
        self.rest_client = rest_client

        self.worker_queue = multiprocessing.JoinableQueue()
        self.workers = multiprocessing.Pool(
            NUM_WORKERS,
            worker_main,
            (rest_client.clone(), self.worker_queue)
        )

    def walk_tree(self, start_path: str, out_file_path: str) -> None:
        """Walk the entire Qumulo cluster, starting from start_path"""

        # Ensure the starting path ends with a '/'
        if not start_path.endswith('/'):
            start_path += '/'

        starting_dir = Directory(start_path, MAX_DEPTH)
        self.worker_queue.put(starting_dir)
        self.worker_queue.join()

        with open(out_file_path, 'wb') as out_file:
            for worker_file_path in glob.glob('out-*.txt'):
                with open(worker_file_path, 'rb') as worker_file:
                    out_file.write(worker_file.read())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Recursive parallel tree walk with Qumulo API'
    )
    parser.add_argument(
        '-d',
        '--starting-directory',
        required=False,
        help='Starting directory',
        default='/'
    )
    parser.add_argument(
        '-o',
        '--out-file-path',
        required=False,
        help='Path to output file to dump results',
        default='file-list.txt'
    )
    parser.add_argument(
        '-p',
        '--password',
        required=True,
        help='Qumulo API password'
    )
    parser.add_argument(
        '-s',
        '--host',
        required=True,
        help='Qumulo cluster ip/hostname'
    )
    parser.add_argument(
        '-u',
        '--username',
        required=False,
        help='Qumulo API password',
        default='admin'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rest_client = RestClient(args.host, REST_PORT)
    rest_client.login(args.username, args.password)

    tree_walker = ApiTreeWalker(
            args.host, args.username, args.password, rest_client)
    tree_walker.walk_tree(args.starting_directory, args.out_file_path)


if __name__ == '__main__':
    main()
