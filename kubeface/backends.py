import collections
from . import local_process_backend, kubernetes_backend

BACKENDS = collections.OrderedDict([
    ('local-process', local_process_backend.LocalProcessBackend),
    ('kubernetes', kubernetes_backend.KubernetesBackend),
])


def add_args(parser):
    group = parser.add_argument_group("kubeface backend")
    group.add_argument(
        "--backend",
        choices=tuple(BACKENDS),
        default=tuple(BACKENDS)[0])

    for (backend, klass) in BACKENDS.items():
        group = parser.add_argument_group("kubeface %s backend" % backend)
        klass.add_args(group)
    return parser


def backend_from_args(args):
    return BACKENDS[args.backend].from_args(args)
