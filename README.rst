kubeface
======

Python library for parallel for loops running directly on kubernetes. Intended for running many expensive tasks (>1 minute in length).

This is currently being spec'd out. Nothing exists!


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

    gcloud container clusters create kubeface-cluster \
        --zone us-east1-c \
        --num-nodes=2 \
        --enable-autoscaling --min-nodes=1 --max-nodes=100 \
        --machine-type=n1-standard-1

You should see your cluster: https://console.cloud.google.com/kubernetes/list

Then run this to set it as the default for your session:

::

    gcloud config set container/cluster kubeface-cluster
    gcloud container clusters get-credentials kubeface-cluster


Now launch a command:

::

    



