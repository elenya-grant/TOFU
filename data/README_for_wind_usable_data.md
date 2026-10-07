# Wind Usable Area by Turbine Class — Parcel-Level Export (2026-10-06)

US manufacturing parcels with wind usable (developable) ground area estimated
for five turbine geometries. One row per parcel.

| | |
|---|---|
| Rows | 306,339 parcels (one per `PARCEL_LID`) |
| Columns | 46 |
| CONUS | 305,123 parcels, directly extracted from the reV composite |
| Alaska | 318 parcels, transfer-ratio imputed |
| Hawaii | 898 parcels, transfer-ratio imputed |
| Source layers | NLR reV wind composites, delivered 2026-10-06, reference scenario |
| CSV md5 | `a54a7633ac697f150523fcb9bad13069` |

Files in this folder:

- `wind_usable_area_by_turbine_class_parcel_10_6_26.csv` — the data
- `COLUMN_DICTIONARY.txt` — every column, with units and definition
- `README.md` — this file

---

## The five turbine classes

| Column suffix | Hub (m) | Rotor (m) | Max tip (m) | Machine scale |
|---|---:|---:|---:|---|
| `bergey_24hh_10rd` | 24 | 9.6 | 28.8 | small distributed (~10 kW class) |
| `npower_37hh_21rd` | 37 | 20.7 | 47.35 | distributed (~100 kW class) |
| `vestas_60hh_47rd` | 60 | 47 | 83.5 | mid-size (~660 kW class) |
| `ge_80hh_77rd` | 80 | 77 | 118.5 | utility (~1.5 MW class) |
| `bespoke_115hh_170rd` | 115 | 170 | 200 | utility-scale anchor (~6 MW class) |

Rotor diameters are the values in the reV setback configuration, which differ
slightly from the rounded names of the two smallest classes (9.6 m not 10,
20.7 m not 21). Turbine manufacturer names are **not** used: the exclusion
configurations specify hub height and rotor diameter only and never name a
machine, so all analysis is keyed on geometry.

Setbacks scale with the machine: in the delivered configuration the generic
setback distance equals maximum tip height exactly (hub + rotor/2), at 2.0x
tip height from structures and 1.1x from rail, road, transmission, water and
pipelines. That is why usable area falls by a factor of ~2.7 from the smallest
to the largest machine.

## Results summary (million m²)

| Scope | bergey_24hh_10rd | npower_37hh_21rd | vestas_60hh_47rd | ge_80hh_77rd | bespoke_115hh_170rd |
|---|---:|---:|---:|---:|---:|
| CONUS (extracted) | 8598.8 | 7679.8 | 6158.8 | 5021.2 | 3212.9 |
| **Alaska — all parcels** | 11671.4 | 10801.6 | 9207.5 | 7884.3 | 5493.3 |
| Alaska — excluding area outliers | 42.1 | 39.5 | 34.6 | 30.7 | 23.4 |
| **Hawaii** | 86.8 | 80.9 | 70.2 | 61.3 | 44.5 |
| National — all parcels | 20357.0 | 18562.4 | 15436.6 | 12966.9 | 8750.7 |
| National — excluding area outliers | 8727.6 | 7800.2 | 6263.7 | 5113.2 | 3280.8 |

**Report Alaska two ways.** Two Alaska parcels are larger than the largest
parcel in the CONUS sample (172,367 acres), so their imputed values are
extrapolations beyond anything observed. They are 99.6%
of the Alaska total for the smallest machine. They are retained in this file
and flagged `flag_area_outlier`; filter on it to get the outlier-free
scope. An intermediate scenario excluding only the larger of the two is **not**
meaningful — 98% of what remains is the other one.

---

## How AK/HI values were produced

The reV wind composites have no Alaska or Hawaii extent, so those 1,216
parcels cannot be extracted and are imputed from CONUS parcels instead.

**Method.** CONUS parcels are grouped into cells, a usable-area-to-parcel-area
ratio is computed per cell, and that ratio is multiplied by each AK/HI parcel's
own area:

```
  ratio(cell)        = SUM(usable area) / SUM(parcel area)   over CONUS parcels in the cell
  usable area(parcel) = ratio(cell) x parcel_area_m2
                        then 0 if the parcel is under 1 acre
```

**Cells** are `size stratum x NLCD land-cover group x IECC climate zone`, with
a four-step fallback when a cell holds too few CONUS parcels:

| Level | Cell definition | AK parcels | HI parcels |
|---|---|---:|---:|
| `L1_size_iecc_nlcd` | size x IECC x NLCD | 103 | 0 |
| `L2_size_nlcd` | size x NLCD | 70 | 378 |
| `L3_size_iecc` | size x IECC | 7 | 0 |
| `L4_size` | size only | 4 | 26 |
| `below_1ac` | zeroed by the 1-acre rule | 134 | 494 |

Every parcel's level is recorded in `impute_level`, and the cell it drew from in
`strata_size_bin` / `nlcd_group` / `iecc_zone`, so any value can be
reconstructed by hand as `ratio_used_<class> x parcel_area_m2`.

### Two deliberate departures from the previously published AK/HI method

**1. Ratio-of-sums replaces the median ratio.** The published method used the
median per-parcel ratio within a cell. Wind usable area is 84-97% zeros across
CONUS, so that median was exactly zero in five of six size strata — which is
the sole reason Hawaii wind was previously reported as 0.00. That was an
artefact of the statistic, not a finding about Hawaii. Ratio-of-sums is the
standard estimator for a total and is unaffected by the zero share.

**2. Land cover is ranked above climate zone.** The published method tried IECC
climate zone before NLCD land cover. All 898 Hawaii parcels fall in IECC zone 1,
whose CONUS counterparts are in south Florida, so 93% of Hawaii's eligible area
being inferred from 18 Miami-Dade and Broward parcels of which only 2 had any
usable area. Wind exclusions are setbacks from infrastructure plus slope and
protected land — all land-cover driven, none driven by thermal climate — so the
ordering was swapped. This single change accounts for most of the difference in
the Hawaii figure.

The previously published estimator is retained per parcel in the
`wind_usable_sqm_median_*` columns for comparison. **It should not be used as
the primary value**; see the accuracy section.

---

## Accuracy and correct use

The method was validated by 10-fold cross-validation on 211,691 CONUS parcels,
where the true extracted value is known. Ratio tables were rebuilt in each fold
so no parcel contributed to its own prediction.

**Valid for aggregates. Not valid for individual sites.**

| Scale | Finding |
|---|---|
| CONUS total | unbiased: +0.13% to -4.43% across the five classes |
| Within each size stratum | unbiased: 0.15% to 1.6% |
| Alaska cell composition | **biased high by 21-25%** |
| Hawaii cell composition | -2.6% to +6.7% |
| Individual parcel | RMSE 12-29x the mean; Spearman 0.29-0.45 |
| Previously published median estimator | biased low by 14-83% |

**Why individual parcels cannot be predicted.** The method assigns one
inclusion fraction to every parcel in a cell, while 84-97% of real parcels have
zero usable area. It estimates how much developable area a *population* of
parcels holds, not which parcels hold it. Do not use an AK/HI row to
characterise a specific site.

**Why Alaska runs high.** Not an estimator problem — it is unbiased within
every stratum. The strata are wide and Alaska sits at a different point inside
them: in the `>=200ac` stratum Alaska's median parcel is 557 acres against
CONUS's 373, and that one stratum spans 200 to 172,367 acres. Treat Alaska as
biased high by roughly 20-25%, or use the calibrated range below.

### Calibrated ranges for the AK/HI aggregates

Derived from 1,000 composition-matched held-out samples. These supersede any
narrower interval: a jackknife standard error on the cell ratio covers only
part of the error and was found to achieve 31-53% coverage against a nominal
95%, because it omits the realised scatter of the particular parcels being
imputed.

| Turbine class | Hawaii | Alaska (excl. area outliers) |
|---|---|---|
| `bergey_24hh_10rd` | 86.8 (43.7 – 127.5) | 42.1 (9.7 – 70.6) |
| `npower_37hh_21rd` | 80.9 (41.5 – 127.7) | 39.5 (8.5 – 67.3) |
| `vestas_60hh_47rd` | 70.2 (32.9 – 104.9) | 34.6 (7.5 – 65.5) |
| `ge_80hh_77rd` | 61.3 (26.2 – 101.1) | 30.7 (4.9 – 55.4) |
| `bespoke_115hh_170rd` | 44.5 (15.4 – 79.3) | 23.4 (1.9 – 45.8) |

The range widens with rotor diameter, because the share of parcels with zero
usable area rises from 84% to 97% and there is correspondingly less to go on.

Million m², 90% range. The Alaska all-parcels figure is **not** calibrated:
it is dominated by the two area-outlier parcels, and no CONUS experiment can
bound an extrapolation.

---

## Using the file

```r
# CONUS only, directly extracted
d |> filter(wind_usable_source == "rev_wind_10_6_26")

# National, excluding the two Alaska area outliers (recommended)
d |> filter(!flag_area_outlier)

# AK/HI imputed rows only
d |> filter(wind_usable_source != "rev_wind_10_6_26")

# Reconstruct any AK/HI value by hand
d |> mutate(check = ratio_used_bergey_24hh_10rd * parcel_area_m2)
```

**Parcel-level, additive.** Values are per parcel. For a parcel with several
facilities, energy demand and load are additive at the parcel level and
`Final_MECS_Matched_kWh` is already the parcel sum; any split back to
individual facilities is a downstream step and is not represented here.

**Joining on FIPS.** `FIPS_CODE` has no leading zeros — Alaska reads `2xxx`,
not `02xxx`. Zero-pad to five characters before joining to any FIPS lookup.
Derive state from `FIPS_CODE` rather than `SITE_STATE`, which is null on some
parcels.

**`size_bin` and `strata_size_bin` are different**. The first is the canonical
five-level reporting class; the second is the six-level stratum used for
imputation. Do not conflate them.

## Caveats

1. **Alaska and Hawaii are modelled, not measured.** The reV layers have no
   AK/HI extent. Those 1,216 rows are inferred from CONUS analogues.
2. **Alaska is biased high by ~20-25%** and dominated by parcel geometry rather
   than by estimation choices. Two parcels are larger than every parcel in
   the CONUS sample.
3. **Land-cover vintage differs.** CONUS parcels use NLCD 2021; Alaska uses 2011
   and Hawaii 2001, the most recent available for those states. The AK/HI
   stratifier is therefore 10-25 years older than the CONUS source.
4. **The largest unquantified uncertainty is whether CONUS cells are the right
   analogue at all.** The validation assumes they are; it cannot test it. If
   Hawaiian ranchland does not behave like its CONUS land-cover counterpart,
   nothing here would detect it.
5. **One reference scenario only.** No open or limited siting-stringency
   variants were produced for the four sub-utility-scale classes.
6. **A turbine-agnostic 1-acre minimum** applies to all five classes. The
   geometry-matched array-envelope alternative changes totals by under 3.3%.
7. **Wind only.** Solar usable area is unchanged and is not in this export.

## Handling

Parcel-level records with centroid coordinates. LightBox assessor and valuation
fields are deliberately excluded to stay inside the licence envelope used for
the published dataset; see the data dictionary for the full exclusion list.
Confirm the destination before redistributing outside PNNL.
