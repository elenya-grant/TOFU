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

export LD_LIBRARY_PATH=/opt/cray/libfabric/1.15.2.0/lib64:$CRAY_MPICH_DIR/lib:$LD_LIBRARY_PATH
export TMPDIR=/tmp/scratch_tofu_$$
mkdir -p "$TMPDIR"

TOFU_DIR="/projects/iedo00onsite/TOFU"
PYTHON=$(which python)
NTASKS=$(( SLURM_NNODES * SLURM_NTASKS_PER_NODE ))

echo "============================================================"
echo "TOFU Pipeline - $(date)"
echo "Nodes: ${SLURM_NNODES}, Tasks/node: ${SLURM_NTASKS_PER_NODE}, Total ranks: ${NTASKS}"
echo "Python: ${PYTHON}"
echo "============================================================"

echo ""
echo ">>> Phase 3.5: Build Wind Sitelist for Resource Downloads"
echo "    Started: $(date)"
$PYTHON "${TOFU_DIR}/tofu/wind_resource/make_sitelist_for_wind_sites.py"
echo "    Completed: $(date)"

# ============================================================
# Phase 4: Download Hourly Wind Resource Data (MPI parallel)
#   - Downloads full-year hourly wind data per site
#   - Output: {match_id}-{year}-{hub_height}m.pkl files
# ============================================================
echo ""
echo ">>> Phase 4: Download Hourly Wind Resource Data (${NTASKS} ranks)"
echo "    Started: $(date)"
srun --mem=0 -N ${SLURM_NNODES} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} \
    $PYTHON "${TOFU_DIR}/tofu/wind_resource/download_wind_resource_data_mpi.py"
echo "    Completed: $(date)"

# ============================================================
# Cleanup
# ============================================================
rm -rf "$TMPDIR"

echo ""
echo "============================================================"
echo "TOFU Pipeline Complete - $(date)"
echo "============================================================"