import collections
from . import local_process_backend
from . import local_process_docker_backend
from . import kubernetes_backend

BACKENDS = collections.OrderedDict([
    ('local-process', local_process_backend.LocalProcessBackend),
    ('local-process-docker',
        local_process_docker_backend.LocalProcessDockerBackend),
    ('kubernetes', kubernetes_backend.KubernetesBackend),
])


def add_args(parser):
    parser.add_argument(
        "--backend",
        choices=tuple(BACKENDS),
        default=tuple(BACKENDS)[0])

    for (backend, klass) in BACKENDS.items():
        klass.add_args(parser)
    return parser


def backend_from_args(args):
    return BACKENDS[args.backend].from_args(args)
