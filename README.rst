kubeface
======

Python library for parallel for loops running directly on kubernetes. Intended for running many expensive tasks (minutes in runtime).


Installation
-------------

From a checkout:

::

    pip install -e .

To run the tests:

::

    # Setting this environment variable is optional.
    # If you set it in the tests will run against a real google storage bucket.
    KUBEFACE_BUCKET=kubeface  # tests will write to gs://kubeface.

    # Run tests:
    nosetests


Shell Example
------------------

The ``kubeface-run-job`` command runs a job from the shell, which is useful for testing or simple tasks.  

If you don't already have a kubernetes cluster running, use a command like this to start one:

::
    gcloud config set compute/zone us-east1-c
    gcloud container clusters create kubeface-cluster-$(whoami) \
        --scopes storage-full \
        --zone us-east1-c \
        --num-nodes=2 \
        --enable-autoscaling --min-nodes=1 --max-nodes=100 \
        --machine-type=n1-standard-16

You should see your cluster listed here: https://console.cloud.google.com/kubernetes/list

Then run this to set it as the default for your session:

::

    gcloud config set container/cluster kubeface-cluster-$(whoami)
    gcloud container clusters get-credentials kubeface-cluster-$(whoami)


Now launch a command:

::

    kubeface-run-job \
        --expression 'value**2' \
        --generator-expression 'range(10)' \
        --max-simultaneous-tasks 10 \
        --backend kubernetes \
        --kubernetes-image hammerlab/kubeface-misc \
        --kubernetes-image-pull-policy Always \
        --kubernetes-task-resources-cpu 1 \
        --kubernetes-task-resources-memory-mb 500 \
        --verbose \
        --out-csv /tmp/result.csv


If you kill the above command, you can run this to kill all the running pods in your cluster:

::

    kubectl delete pods --all


When you're done working, delete your cluster:

::

    gcloud container clusters delete kubeface-cluster-$(whoami)


