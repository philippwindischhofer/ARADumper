#!/bin/bash

NUPHASE_INSTALL_DIR=/home/windischhofer/ARA/nuphaseroot_install
export ROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source /project/avieregg/software/midway3-setup.sh
module load cmake
module load ROOT
module load python
module load intel
export LD_LIBRARY_PATH=$NUPHASE_INSTALL_DIR/lib:$LD_LIBRARY_PATH
