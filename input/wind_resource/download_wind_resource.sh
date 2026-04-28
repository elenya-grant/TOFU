#!/bin/bash                                                                                                             
#SBATCH --job-name=tofu_wind_rsrc_dnwload
#SBATCH --output=R-%x.%j.out
#SBATCH --partition=standard
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=78
#SBATCH --time=4:00:00
#SBATCH --account=iedo00onsite
#SBATCH --mail-user dbernal@nlr.gov
#SBATCH --mail-type BEGIN,END,FAIL
module load conda
conda activate /home/dbernal/.conda-envs/tofu
module load cray-mpich
export TMPDIR=/scratch/egrant/sc_tmp/
srun -N 8 --ntasks-per-node=78 /home/dbernal/.conda-envs/tofu/bin/python /projects/iedo00onsite/TOFU/tofu/wind_resource/download_wind_resource_data_mpi.py