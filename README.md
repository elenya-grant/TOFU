# TOFU
project code for onsite energy geospatial analysis

# Installation Instructions (local):
Step 1. create a fork and clone that or clone this repo
    ```
    git clone https://github.com/elenya-grant/TOFU.git
    cd TOFU
    ```

Step 2. create environment
    ```
    conda create --name tofu_env python=3.8 -y
    conda activate tofu_env
    ```

Step 3. install dependencies
    ```
    conda install geopandas
    pip install -r requirements.txt
    ```

Step 4. (optional - for HPC parallel runs) install mpi4py
    ```
    pip install -e .
    ```

Step 5. finish install:
    ```
    pip install -e .
    ```

# Installation Instructions (HPC):
1. navigate to directory and clone repo
    ```
    cd scratch/<user>
    git clone git@github.com:elenya-grant/TOFU.git
    cd TOFU
    ```
2. Create and activate conda environment
    ```
    module load conda
    export CONDA_PKGS_DIRS=/scratch/<user>/.conda-pkgs
    conda create --prefix /scratch/<user>/tofu_env python=3.10
    conda activate /scratch/<user>/tofu_env
    ```
3. install mpi4py
    ```
    module load cray-mpich
    ```
    - check the mpicc, following command should return ``/opt/cray/pe/mpich/8.1.28/ofi/crayclang/16.0/bin/mpicc``
        ```
        which mpicc
        ```
    - install mpi4py
        ```
        python -m pip install mpi4py
        ```
    - NOTE: if theres an issue installing mpi4py, then try running the below codeL
        ```bash
        #some random step required with newer python versions
        rm /scratch/<user>/tofu_env/compiler_compat/ld 
        # try to re-install mpi4py
        python -m pip install mpi4py
        ```
4. install dependencies
    ```
    pip install -r requirements.txt
    ```
5. finalize setup
    ```
    pip install -e .
    ```
6. make temporary directory for any mpi runs for extra memory:
    ```
    cd scratch/<user>
    mkdir sc_tmp
    ```
### Steps to run siting and getting resource:
1. run wind siting (this can be done on a local machine, not HPC)
    ```
    python tofu/wind_siting_analysis/run_wind_siting.py
    ```
2. get wind resource GIDs from wind toolkit (suggested to use interactive session on HPC)
    ```
    cd scratch/<user>
    salloc --time=60 --account=<project allocation> --ntasks=2 --partition=shared --mem=20GB
    module load conda
    conda activate /scratch/<user>/tofu_env
    module load cray-mpich
    export TMPDIR=/scratch/<user>/sc_tmp/
    srun -n 2 /scratch/<user>/tofu_env/bin/python /scratch/<user>/TOFU/tofu/site_resource_analysis/run_site_gids_mpi.py True False
    ```
3. download and save wind resource data (MPI)
    - example batch script lives here: `input/wind_resource/download_wind_resource.sh`