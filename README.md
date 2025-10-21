# Diaspora Stream Template for C++

This repository contains a simple example implementation of a streamining
backend for the [Diaspora Stream API](https://github.com/diaspora-project/diaspora-stream-api).

## Using this template

To get started, click the [Use template](https://github.com/new?owner=diaspora-project&template_name=diaspora-stream-template-cpp&template_owner=diaspora-project) button (or this link) to create a new project using
the template. Upon creation, your new project will run a GitHub workflow but it will fail.

Navigate your new project's Settings tab, click on Actions, then General, under "Workflow permissions"
select "Read and write permissions", and save.

Then, navigate to the Actions tab, click on the failed workflow run, then click on Re-run all jobs.
This will run the Github workflow again and this time it should correctly rename the files and classes.

## Building your project

Once your project is setup and cloned locally, use [spack](https://spack.readthedocs.io/en/latest/)
to install its dependencies as follows.

```
$ git clone https://github.com/mochi-hpc/mochi-spack-packages.git
$ spack env create my-env spack.yaml
$ spack env activate my-env
$ spack repo add mochi-spack-packages
$ spack install
```

You can now build the project as follows.

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

# Implementing your interface

This project contains classes respectively implementing Diaspora's
DriverInterface, TopicHandleInterface, ThreadPoolInterface, ProducerInterface,
ConsumerInterface, and EventInterface.

Simply edit their code as needed.

# Testing your implementation

This part is in progress...
