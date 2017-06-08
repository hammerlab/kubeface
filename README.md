kubeface
[![Build Status](https://travis-ci.org/hammerlab/kubeface.svg?branch=master)](https://travis-ci.org/hammerlab/kubeface)
========

Python library for parallel maps running directly on Kubernetes. Intended for running many expensive tasks (minutes in runtime). Alpha stage. Currently supports only Google Cloud.

Overview
========

Kubeface aims for reasonably efficient execution of many long running Python tasks with medium sized (up to a few gigabytes) inputs and outputs. Design choices and assumptions:

* Each task runs in its own bare kubernetes pod. There is no state shared between tasks
* All communication is through Google Storage Buckets
* Each task's input and output must fit in memory, but we do not assume that more than one task's data fits simultaneously
* Work performed as part of jobs that crash can be re-used for reruns
* We favor debuggability over performance

The primary motivating application has been neural network model selection for the [MHCflurry](https://github.com/hammerlab/mhcflurry) project.

See [example.py](example.py) for a simple working example.

Nomenclature
-------------

* **Master:** the Python process the user launches. It uses kubeface to run *jobs*
* **Worker:** a process running external to the master (probably on a cluster) that executes a *task*
* **Job:** Each call to `client.map(...)` creates a *job*
* **Task:** Each invocation of the function given to map is a *task*

Backends
-------------

* The *kubernetes* backend runs tasks on Kubernetes. This is what is used in production
* The *local-process* backend runs tasks as local processes. Useful for development and testing of both kubeface and code that uses it
* The *local-process-docker* backend runs tasks as local processes in a docker container. This is used for testing kubeface


Life of a job
-------------

If a user calls (where `client` is a [kubeface.Client](kubeface/client.py) instance):

```py
client.map(lambda x: x**2, range(10))
```

This creates a *job* containing 10 *tasks*. The return value is a generator that will yield the square of the numbers 0-9. The job is executed as follows:

* Submission: for each task:
  * an input file containing a pickled (we use the [dill](https://github.com/uqfoundation/dill) library) representation of the task's input is uploaded to cloud storage. In this example the input data is a number 0-9.
  * A `kubectl` command is issued that creates a bare pod whose entrypoint (i.e. what runs in the pod) installs kubeface if necessary then calls the command `_kubeface-run-task <input-path> <output-path>`.
  * The `_kubeface-run-task` command downloads the input file from cloud storage, runs the task, and uploads the result to the specified path.
* After all tasks have been submitted, kubeface waits for all results to appear in cloud storage. It may speculatively re-submit some tasks that appear to be straggling or crashed.
* Once all results are available, each task’s result is read by the master and yielded to the client code


Docker images
-------------

Kubeface tasks execute in the context of a particular docker image, since they run in a kubernetes pod. You can use any docker image with python installed. If your docker image does not have kubeface installed, then by default kubeface will try to install itself using `pip`. This is inefficient since it will run for every task. If you plan on running many tasks it's a good idea to create your own docker image with kubeface installed.

Inspecting job status
----------------------
Kubeface writes out HTML and JSON status pages to cloud storage and logs to stdout. However, the best way to figure out what's going on with your job is to use kubernetes directly, via `kubectl get pods` and `kubectl logs <pod-name>`.


Installation
============

From a checkout:

    pip install -e .

To run the tests:

    # Setting this environment variable is optional.
    # If you set it in the tests will run against a real google storage bucket.
    # See https://developers.google.com/identity/protocols/application-default-credentials#howtheywork;
    # you need to get Application Default Credentials before writing to your bucket.
    KUBEFACE_STORAGE=gs://kubeface-test  # tests will write to gs://kubeface-test.

    # Run tests:
    nosetests

Shell Example
=============

The `kubeface-run` command runs a job from the shell, which is useful for testing or simple tasks.

If you don’t already have a kubernetes cluster running, use a command like this to start one:

    gcloud config set compute/zone us-east1-c
    gcloud components install kubectl  # if you haven't already installed kubectl
    gcloud container clusters create kubeface-cluster-$(whoami) \
        --scopes storage-full \
        --zone us-east1-c \
        --num-nodes=2 \
        --enable-autoscaling --min-nodes=1 --max-nodes=100 \
        --machine-type=n1-standard-16

You should see your cluster listed here: <https://console.cloud.google.com/kubernetes/list>

Then run this to set it as the default for your session:

    gcloud config set container/cluster kubeface-cluster-$(whoami)
    gcloud container clusters get-credentials kubeface-cluster-$(whoami)

Now launch a command:

    kubeface-run \
        --expression 'value**2' \
        --generator-expression 'range(10)' \
        --kubeface-max-simultaneous-tasks 10 \
        --kubeface-backend kubernetes \
        --kubeface-worker-image continuumio/anaconda3 \
        --kubeface-kubernetes-task-resources-cpu 1 \
        --kubeface-kubernetes-task-resources-memory-mb 500 \
        --verbose \
        --out-csv /tmp/result.csv

If you kill the above command, you can run this to kill all the running pods in your cluster:

    kubectl delete pods --all

When you’re done working, delete your cluster:

    gcloud container clusters delete kubeface-cluster-$(whoami)

