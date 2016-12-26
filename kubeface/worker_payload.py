import zipfile
import pkgutil
import tempfile
import base64

REQUIRED_FILES = [
    'serialization.py',
    'storage.py',
    'common.py',
    'bucket_storage.py',
    'commands/run_task.py',
]

PAYLOAD_PATH = "/tmp/kubeface_payload.egg"


def payload_python_args():
    with tempfile.NamedTemporaryFile(suffix=".zip") as fd:
        with zipfile.ZipFile(fd.name, "w") as zfd:
            zfd.writestr("kubeface_payload/__init__.py", "")
            zfd.writestr("kubeface_payload/commands/__init__.py", "")
            for filename in REQUIRED_FILES:
                zfd.writestr(
                    "kubeface_payload/" + filename,
                    pkgutil.get_data("kubeface", filename))
        fd.flush()
        fd.seek(0)
        encoded = base64.b64encode(fd.read()).decode()

    code = """
    import sys, base64, io
    fd = open("{PAYLOAD_PATH}", "wb")
    fd.write(base64.b64decode(sys.argv[1]))
    fd.close()
    sys.path.insert(0, "{PAYLOAD_PATH}")
    from kubeface_payload.commands import run_task
    run_task.run(sys.argv[2:])
    """.strip().replace("\n", "; ").format(
        PAYLOAD_PATH=PAYLOAD_PATH)
    return [
        'python',
        '-c',
        code,
        encoded,
    ]

