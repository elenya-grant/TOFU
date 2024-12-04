# TOFU
project code for onsite energy geospatial analysis

# Installation Instructions:
Step 1. create a fork and clone that or clone this repo
    ```
    git clone https://github.com/elenya-grant/TOFU.git
    cd TOFU
    ```

Step 2. create environment
    ```
    conda create --name tofu python=3.9 -y
    conda activate tofu
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