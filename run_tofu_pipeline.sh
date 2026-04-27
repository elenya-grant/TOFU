#!/bin/bash
#SBATCH --job-name=tofu_pipeline
#SBATCH --output=R-%x.%j.out
#SBATCH --partition=standard
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=52
#SBATCH --time=8:00:00
#SBATCH --account=iedo00onsite
#SBATCH --mail-user dbernal@nrel.gov
#SBATCH --mail-type BEGIN,END,FAIL

set -e  # exit on any error

module purge

# ============================================================
# Environment setup
# ============================================================
module load conda
conda activate /home/dbernal/.conda-envs/tofu
module load cray-mpich
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

# ============================================================
# Phase 1: Wind Siting Analysis (single process, no MPI)
#   - Filters sites for 5x5 and 3x7 turbine layouts
#   - Cleans and ranks sitelists
#   - Output: TOFU/results/wind_siting_analysis/
# ============================================================
echo ""
echo ">>> Phase 1: Wind Siting Analysis"
echo "    Started: $(date)"
$PYTHON "${TOFU_DIR}/tofu/wind_siting_analysis/run_wind_siting.py"
echo "    Completed: $(date)"

# ============================================================
# Phase 2: Extract Site Resource GIDs (MPI parallel)
#   - Maps lat/lon to WTK and NSRDB grid IDs
#   - Output: site_gids--_*.pkl files
# ============================================================
echo ""
echo ">>> Phase 2: Extract Site Resource GIDs (${NTASKS} ranks)"
echo "    Started: $(date)"
srun -N ${SLURM_NNODES} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} \
    $PYTHON "${TOFU_DIR}/tofu/site_resource_analysis/run_site_gids_mpi.py"
echo "    Completed: $(date)"

# Phase 2 post-processing: combine per-rank GID files
echo ""
echo ">>> Phase 2 Post-Processing: Combine GID results"
echo "    Started: $(date)"
$PYTHON "${TOFU_DIR}/tofu/site_resource_analysis/process_site_gids_results.py"
echo "    Completed: $(date)"

# ============================================================
# Phase 3: Extract Resource Summaries (MPI parallel)
#   - Pulls wind/solar resource data from HPC H5 datasets
#   - Output: wind_site_resource--_*.pkl, solar_site_resource--_*.pkl
# ============================================================
echo ""
echo ">>> Phase 3: Extract Wind Resource Summaries (${NTASKS} ranks)"
echo "    Started: $(date)"
srun -N ${SLURM_NNODES} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} \
    $PYTHON "${TOFU_DIR}/tofu/site_resource_analysis/run_site_resource_mpi.py"
echo "    Completed: $(date)"

# Phase 3 post-processing: combine per-rank resource files
echo ""
echo ">>> Phase 3 Post-Processing: Combine resource results"
echo "    Started: $(date)"
$PYTHON "${TOFU_DIR}/tofu/site_resource_analysis/process_resource_results.py"
echo "    Completed: $(date)"

# ============================================================
# Phase 3.5: Build wind-specific sitelist for downloads
#   - Joins siting results (Phase 1) with GIDs (Phase 2)
#   - Maps turbines to hub heights
#   - Output: wind_sites_for_resource_download_{layout}.pkl
# ============================================================
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
srun -N ${SLURM_NNODES} --ntasks-per-node=${SLURM_NTASKS_PER_NODE} \
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
