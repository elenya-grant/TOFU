#!/bin/bash                                                                                                             
#SBATCH --job-name=tofu_wind_rsrc_dnwload
#SBATCH --output=R-%x.%j.out
#SBATCH --partition=standard
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=78
####SBATCH --time=48:00:00
#SBATCH --time=4:00:00
#SBATCH --account=iedo00onsite
#SBATCH --mail-user egrant@nrel.gov
#SBATCH --mail-type BEGIN,END,FAIL
module load conda
conda activate /scratch/egrant/tofu_env
module load cray-mpich
export TMPDIR=/scratch/egrant/sc_tmp/
srun -N 8 --ntasks-per-node=78 /scratch/egrant/tofu_env/bin/python /scratch/egrant/TOFU/tofu/wind_resource/download_wind_resource_data_mpi.py