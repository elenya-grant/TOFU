"""One-time augmentation: attach obs_id from the old facility-level sitelist onto the
new parcel-level wind usable-area CSV, matched on PARCEL_LID.

A PARCEL_LID may hold several facilities in the old file; their obs_id values are
collapsed into a single comma-joined string so the column stays hashable downstream.
Parcels with no match keep the row with an empty obs_id.

Run once after receiving a new parcel-level CSV:
    python -m tofu.utilities.augment_sitelist_with_obs_id
"""
import os
import pandas as pd
from tofu import DATA_DIR

OLD_SITELIST_DEFAULT = os.path.join(str(DATA_DIR), "aggregated_facility_level_site_list_2026_04_23.csv")
NEW_SITELIST_NAME = "wind_usable_area_by_turbine_class_parcel_10_6_26.csv"

# Every location the pipeline reads the new CSV from; each is augmented in place.
NEW_SITELIST_TARGETS = [
    os.path.join(str(DATA_DIR), NEW_SITELIST_NAME),
    os.path.join(
        "/projects/iedo00onsite/onsite-energy-analysis/data/pnnl_parcel_land_coverage_data/updated_10_7_2026",
        NEW_SITELIST_NAME,
    ),
]


def build_obs_id_by_parcel(old_sitelist_path=OLD_SITELIST_DEFAULT):
    old = pd.read_csv(old_sitelist_path, usecols=["PARCEL_LID", "obs_id"], encoding="ISO-8859-1")
    old = old.dropna(subset=["obs_id"])
    return old.groupby("PARCEL_LID")["obs_id"].apply(
        lambda s: ", ".join(dict.fromkeys(str(x) for x in s))
    )

# Calculate a single usable_wind_sqm by getting the highest value across all turbine classes - do not replace any, just append to df
def add_usable_wind_sqm(df):
    turbine_cols = [c for c in df.columns if c.startswith("usable_wind_sqm_")]
    if turbine_cols:
        df["usable_wind_sqm"] = df[turbine_cols].max(axis=1)
    else:
        df["usable_wind_sqm"] = 0
    return df


def augment_sitelist_with_obs_id(new_sitelist_path, old_sitelist_path=OLD_SITELIST_DEFAULT, output_path=None):
    output_path = output_path or new_sitelist_path
    obs_by_parcel = build_obs_id_by_parcel(old_sitelist_path)
    df = pd.read_csv(new_sitelist_path)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    df["obs_id"] = df["PARCEL_LID"].map(obs_by_parcel)
    df = add_usable_wind_sqm(df)
    df.to_csv(output_path, index=False)
    matched = int(df["obs_id"].notna().sum())
    print("augmented {}: {}/{} parcels got obs_id".format(output_path, matched, len(df)))
    return output_path


if __name__ == "__main__":
    for target in NEW_SITELIST_TARGETS:
        if os.path.exists(target):
            augment_sitelist_with_obs_id(target)
        else:
            print("skipping missing file: {}".format(target))
