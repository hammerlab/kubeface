# Design

## Motivation

We would like to run fairly long-running Python tasks over Kubernetes on Google Cloud.

Two applications we need this for are MHCflurry model selection and data preparation for antigen presentation predictors, where we would like to run some analyses over the full peptidome.

We have previously experimented with an approach based on running [dask-distributed](https://github.com/dask/distributed) on Kubernetes as described [here](https://github.com/dask/distributedhttps://github.com/hammerlab/dask-distributed-on-kubernetes).

However, having long running server processes as in dask distributed has resulted in reliability issues for us. When results are large the distributed scheduler seems to slow down or crash. Since we don't care about latency, I think it would be less errorprone to run each task in its own Kubernetes job and use Google Buckets to shuffle data around.

Our MHCflurry code can use any parallel map implementation, see e.g. [here](https://github.com/hammerlab/mhcflurry/blob/master/mhcflurry/class1_allele_specific/train.py#L308). We should be able to make a library that plugs in there without any significant modification to MHCflurry.

Design parameters
 * There's a master process, which the user launches. It calls a parallel map impementation to do work on the cluster.
 * Tasks are independent, do not communicate
 * Long running tasks, say 5 min - 5 hours.
 * Many tasks: as many as 10k.
 * Significant data exchange. Input and result to *each task* may be as high as 1gb. Full input dataset to all tasks does not fit in memory on any node. Full result set across tasks
 also does not fit. Input and result from any single task fits on all nodes.
 * No attempt at recovery if a task throws a Python exception. Kill the whole run.

The main goal here is simplicity and reliability. We do not care at all about latency; fine if it takes 5 minutes for Kubernetes to launch a task. We want to push all tricky issues, in particular scheduling of tasks and recovery of failed nodes, onto Kubernetes. We should never have two python processes talking directly to each other. We should only interact with Kubernetes and Google Storage Buckets. 

## Interface

This project should expose a library that implements a parallel map, e.g.

```python
def parallel_map(func, iterable):
    """
    Parallel map. Each invocation of func is run in its own kubernetes Job.

    Returns (func(x) for x in iterable)
    """
```

There is some configuration that is shared across invocations of parallel_map, so it makes sense to put this in a class and then have parallel_map as a method of it, e.g.

```python
class Client(object):
    def __init__(
            self,
            image,
            bucket,
            image_pull_policy="Always",
            cluster=None,
            available_parallelism=None,
            python_path='/usr/bin/env python',
            run_locally=False):
            """
            Create a client for running tasks on Kubernetes.

            Parameters
            --------------

            image : string
                Docker image to use (on docker hub)

            image_pull_policy : boolean, optional
                Kubernetes imagePullPolicy setting. See [1]

            cluster : string
                Kubernetes cluster to schedule jobs on

            available_parallelism : int
                If specified, max number of jobs to schedule on Kubernetes at once

            python_path : string
                Path to Python binary in the image

            run_locally: boolean
                Run tasks in the current process. Useful for testing

            [1] http://kubernetes.io/docs/user-guide/images/
            """

    def parallel_map(func, iterable):
    ...
```


## Implementation

Possible first-pass implementation. For each task (this is running on the master node):

 * Serialize the function to run and its input (using e.g. [dill](https://github.com/uqfoundation/dill))
 * Copy serialized data to a Google Bucket, give the file a unique name.
 * Schedule a Kubernetes job that runs a Python script that downloads the serialized data from Google Bucket, unserializes it, runs the function on the data, and copies the serialized result to a unique filename on the Google Bucket

Then the master node would poll for the results on Google Bucket, and perhaps issue Kubernetes commands to watch what's been scheduled etc. and report the progress to the user.

We can either issue kubernetes and gsutil commandline calls directly or interact with them through their REST APIs using a project like [pykube](https://github.com/kelproject/pykube).

Kubernetes [secrets](http://kubernetes.io/docs/user-guide/secrets/) may be an alternative approach to sending each task its input data.


## Unknowns

 * Is Google Bucket going to hold up to having tons of tasks hitting it with downloads and uploads? Is it fast enough?
 * Is Kubernetes stable enough?
 * How can we test this library without actually using Google Cloud? [Kubernetes on vagrant](https://coreos.com/kubernetes/docs/latest/kubernetes-on-vagrant-single.html) may be relevant here. Not sure what to do about Google Bucket dependency.

