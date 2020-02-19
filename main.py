from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from logging import getLogger, basicConfig, DEBUG, INFO
from multiprocessing import cpu_count
from os import getcwd

from docker import from_env
from docker.types import Mount

_LOGGER = getLogger(__name__)


def worker(image: str, command: str, i: int, print_out: bool) -> None:
    _LOGGER.info('%s: iteration started', i)

    workdir = '/workdir'

    try:
        c = from_env().containers.run(
            image,
            command,
            environment=[f'ITERATION={i}'],
            mounts=[Mount(workdir, getcwd(), 'bind')],
            working_dir=workdir)

        if print_out:
            lines = c.decode().split('\n')
            for l in lines:
                _LOGGER.debug('%s: %s', i, l)
    except Exception:
        _LOGGER.exception('Error in the Docker container')

    _LOGGER.info('%s: iteration ended', i)


def main() -> None:
    basicConfig(format='%(asctime)-15s %(levelname)s :: %(message)s', level=DEBUG)

    parser = ArgumentParser()

    parser.add_argument('command', type=str, nargs='+')

    parser.add_argument(
        '-i',
        type=str,
        default='gitlab-registry.cern.ch/vavolkl/fcc-ubuntu',
        help='the image to run')

    parser.add_argument('-n', type=int, default=100)
    parser.add_argument('-o', action='store_true')
    parser.add_argument('-w', type=int, default=cpu_count()-1)

    args = parser.parse_args()

    _LOGGER.info('Starting the worker pool')

    with ProcessPoolExecutor(max_workers=args.w) as p:
        for i in range(1, args.n+1):
            p.submit(worker, args.i, args.command, i, args.o)

    _LOGGER.info('pool exiting')


if __name__ == '__main__':
    main()