#!/bin/bash

NUPHASE_INSTALL_DIR=/home/windischhofer/ARA/nuphaseroot_install

source /project/avieregg/software/midway3-setup.sh
module load cmake
module load ROOT
module load python
export LD_LIBRARY_PATH=$NUPHASE_INSTALL_DIR/lib:$LD_LIBRARY_PATH
