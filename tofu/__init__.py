from pathlib import Path

TOFU_DIR = Path(__file__).resolve().parent
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR/"data"
INPUT_DIR = ROOT_DIR/"input"
# OUTPUT_DIR = ROOT_DIR/"outputs"
OUTPUT_DIR = ROOT_DIR/"results"

'''
ONSITE_DIR = ROOT_DIR.parent / "onsite-energy-analysis"

DATA_DIR = ONSITE_DIR/"data"
INPUT_DIR = ONSITE_DIR/"code"/"common"/"input"
# OUTPUT_DIR = ROOT_DIR/"outputs"
OUTPUT_DIR = ONSITE_DIR/"results"
'''