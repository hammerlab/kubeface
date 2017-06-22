import os

from six.moves import shlex_quote as quote


class WorkerConfiguration(object):
    @staticmethod
    def add_args(parser):
        parser.add_argument(
            "--kubeface-worker-image",
            default=DEFAULT.image)
        parser.add_argument(
            "--kubeface-worker-path-prefix",
            default=DEFAULT.path_prefix)
        parser.add_argument(
            "--kubeface-worker-pip",
            default=DEFAULT.pip)
        parser.add_argument(
            "--kubeface-worker-pip-packages",
            default=DEFAULT.pip_packages, nargs="+")
        parser.add_argument(
            "--kubeface-worker-kubeface-install-policy",
            choices=('if-not-present', 'always', 'never'),
            default=DEFAULT.kubeface_install_policy)
        parser.add_argument(
            "--kubeface-worker-kubeface-install-command",
            default=DEFAULT.kubeface_install_command)

    @staticmethod
    def from_args(args):
        arg_prefix = "kubeface_worker_"
        return WorkerConfiguration(
            **dict(
                (key[len(arg_prefix):], value)
                for (key, value) in args._get_kwargs()
                if key.startswith(arg_prefix)))

    def __init__(
            self,
            image='continuumio/anaconda3',
            path_prefix='',
            pip='pip',
            pip_packages=[],
            kubeface_install_policy='if-not-present',

            # TODO: this should default to installing the version of kubeface
            # running in the current process, not HEAD.
            kubeface_install_command=(
                "{pip} install "
                "https://github.com/hammerlab/kubeface/archive/master.zip"
            )):

        if kubeface_install_policy not in (
                'if-not-present', 'always', 'never'):
            raise ValueError(
                "Invalid kubeface_install_policy: %s"
                % kubeface_install_policy)

        self.image = image
        self.path_prefix = path_prefix
        self.pip = pip
        self.pip_packages = pip_packages
        self.kubeface_install_policy = kubeface_install_policy
        self.kubeface_install_command = kubeface_install_command

    def non_default_fields(self):
        return set([
            field for field in dir(self)
            if getattr(self, field) != getattr(DEFAULT, field)
        ])

    def command(self, task_input, task_output, extra_task_args=[]):
        def quote_and_join(arguments):
            return " ".join([quote(arg) for arg in arguments])

        pieces = []
        run_pip = quote(os.path.join(self.path_prefix, 'pip'))
        run_task = quote(
            os.path.join(self.path_prefix, '_kubeface-run-task'))
        kubeface_install_command = self.kubeface_install_command.format(
            pip=run_pip)
        if self.kubeface_install_policy == 'if-not-present':
            # From: http://stackoverflow.com/questions/592620/check-if-a-program-exists-from-a-bash-script
            pieces.append("command -v %s || { %s ; } " % (
                run_task,
                kubeface_install_command))
        elif self.kubeface_install_policy == 'always':
            pieces.append(kubeface_install_command)
        if self.pip_packages:
            pieces.append("%s install %s" % (
                run_pip,
                quote_and_join(self.pip_packages)))
        pieces.append(
            run_task +
            " " +
            quote_and_join([
                task_input,
                task_output,
                "--verbose",
            ] + extra_task_args))
        result = " && ".join(pieces)
        return result


DEFAULT = WorkerConfiguration()
