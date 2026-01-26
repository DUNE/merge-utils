## simple example

### First get set up

This example is from a gpvm at Fermilab and assumes you want to use SL7

#### decide on a name for your project 

~~~
/cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer shell --shell=/bin/bash -B /cvmfs,/exp,/nashome,/pnfs/dune,/opt,/run/user,/etc/hostname,/etc/krb5.conf --ipc --pid /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-dev-sl7:latest
~~~

~~~
export DUNEDATA=/exp/dune/data/users/$USER
cd $DUNEDATA
git clone https://github.com/dune/merge-utils.git
cd merge-utils
git checkout -b <your project name>
source setup.sh # this will create a local virtual environment
~~~

Go to subdirectory `campaigns` and make a subdirectory for your project (can be the same as the branch)

You should do most of your work in this directory. 


