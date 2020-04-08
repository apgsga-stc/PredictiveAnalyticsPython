# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))


from datetime import timedelta
import pandas as pd
import statsmodels.api as sm

data_dir = Path.home() / "data" / "dashboard-corona"
exit(0)

########################################################################################
url_root = (
    "https://data.stadt-zuerich.ch/dataset/"
    + "6212fd20-e816-4828-a67f-90f057f25ddb/resource/"
)
url_2020 = (
    url_root
    + "44607195-a2ad-4f9b-b6f1-d26c003d85a2/download/"
    + "sid_dav_verkehrszaehlung_miv_od2031_2020.csv"
)
url_2019 = (
    url_root
    + "fa64fa70-6328-4d47-bcf0-1eff694d7c22/download/"
    + "sid_dav_verkehrszaehlung_miv_od2031_2019.csv"
)
url_2018 = (
    url_root
    + "d5963dee-7841-4e64-9268-6c850a2fc497/download/"
    + "sid_dav_verkehrszaehlung_miv_od2031_2018.csv"
)
url_2017 = (
    url_root
    + "f873cc29-96ac-4b2f-b175-f733513e4012/download/"
    + "sid_dav_verkehrszaehlung_miv_od2031_2017.csv"
)
url_verkehr = [url_2020, url_2019, url_2018, url_2017]


########################################################################################
def load_verkehr_data(url_list):
    zh_raw_df = pd.concat([pd.read_csv(url, low_memory=False) for url in url_list])
    zh_raw_df = zh_raw_df.astype(
        {"MessungDatZeit": "datetime64[ns]", "LieferDat": "datetime64[ns]"}
    )
    zh_raw_df.loc[:, "hour"] = zh_raw_df.MessungDatZeit.dt.hour
    zh_raw_df.loc[:, "dayofweek"] = zh_raw_df.MessungDatZeit.dt.dayofweek
    zh_raw_df.loc[:, "month_name"] = zh_raw_df.MessungDatZeit.dt.month_name()
    zh_raw_df.loc[:, "year"] = zh_raw_df.MessungDatZeit.dt.year
    zh_raw_df.loc[:, "dayofyear"] = zh_raw_df.MessungDatZeit.dt.dayofyear
    zh_raw_df.loc[:, "hourofyear"] = zh_raw_df.hour + (zh_raw_df.dayofyear - 1) * 24

    print(zh_raw_df.LieferDat.max())

    return zh_raw_df


########################################################################################
# Remove all MSID-machines with a siginificant downtime:


def msid_uptime_okay(zh_verkehr_all: pd.DataFrame) -> pd.DataFrame:
    msid_uptime_hrs_per_year = pd.pivot_table(
        zh_verkehr_all[~zh_verkehr_all.AnzFahrzeuge.isna()],
        index="MSID",
        columns="year",
        values="AnzFahrzeuge",
        aggfunc="count",
    ).reset_index(inplace=False)

    quantiles_uptime_count = msid_uptime_hrs_per_year.quantile(0.90, axis=0) * 0.95

    year_list = list(msid_uptime_hrs_per_year.columns)
    year_list.remove("MSID")
    is_above_quantile = pd.Series(msid_uptime_hrs_per_year.shape[0] * [True])

    for year in year_list:
        is_above_quantile = (
            msid_uptime_hrs_per_year.loc[:, year] >= quantiles_uptime_count[year]
        ) & is_above_quantile

    print(is_above_quantile.value_counts())
    working_msid = list(set(msid_uptime_hrs_per_year.loc[is_above_quantile, "MSID"]))
    return zh_verkehr_all.loc[zh_verkehr_all.MSID.isin(working_msid), :]


########################################################################################
def trend_data(current_year: int, compare_with_year: int, data_set) -> pd.DataFrame:
    temp_copy_df = (
        (
            data_set.loc[
                data_set.Jahr.isin([current_year, compare_with_year]),
                ["Jahr", "Datum_Stunde", "AnzFahrzeuge"],
            ]
        )
        .sort_values("Datum_Stunde")
        .copy()
    )

    pivottable = pd.pivot_table(
        temp_copy_df,
        index="Datum_Stunde",
        values="AnzFahrzeuge",
        columns=["Jahr"],
        # aggfunc=np.sum,
    ).reset_index()

    trend_values = (
        pivottable.loc[:, current_year] / pivottable.loc[:, compare_with_year] * 100
    ).copy()

    pivottable.loc[:, "trend_factor"] = trend_values

    return pivottable.loc[:, ["Datum_Stunde", "trend_factor"]]


########################################################################################

zh_verkehr = load_verkehr_data(url_list=url_verkehr)
zh_verkehr_cleaned = msid_uptime_okay(zh_verkehr_all=zh_verkehr)

########################################################################################
lowess = sm.nonparametric.lowess
lowess_anzfahrzeuge = lowess(
    exog=zh_verkehr_cleaned.MessungDatZeit,
    endog=zh_verkehr_cleaned.AnzFahrzeuge,
    frac=0.0075,  # 0.01,
    return_sorted=False,
    missing="drop",
)
zh_verkehr_cleaned.loc[:, "anzfahrzeuge_lowess"] = lowess_anzfahrzeuge
########################################################################################

prep_01 = zh_verkehr_cleaned.loc[
    :, ["hour", "year", "dayofyear", "anzfahrzeuge_lowess"],
].drop_duplicates()

prep_01.loc[:, "hourofyear"] = prep_01.hour + (prep_01.dayofyear - 1) * 24

prep_02 = pd.pivot_table(
    prep_01, index="hourofyear", columns="year", values="anzfahrzeuge_lowess"
).reset_index()

########################################################################################
datum_referenz = pd.DataFrame(
    [
        (x, pd.to_datetime("01.01.2020", format="%d.%m.%Y") + timedelta(hours=x))
        for x in range(0, 370 * 24)
    ],
    columns=["hourofyear", "Datum_Stunde"],
)

prep_03 = prep_02.merge(
    datum_referenz, how="left", left_on="hourofyear", right_on="hourofyear"
)
########################################################################################

kilometer_columns = [2017, 2018, 2019, 2020]

prep_04 = pd.DataFrame()
for data_column in kilometer_columns:
    temp_df = prep_03.loc[:, ["hourofyear", "Datum_Stunde", data_column]].rename(
        columns={data_column: "AnzFahrzeuge"}
    )
    temp_df.loc[:, "Jahr"] = data_column
    prep_04 = pd.concat([prep_04, temp_df])

verkehrsdaten_lowess = prep_04.reset_index().drop(columns="index")

verkehrsdaten_lowess.to_feather(str(data_dir / "verkehrsdaten_lowess.feather"))


########################################################################################
########################################################################################

verkehrsdaten_trend = trend_data(
    current_year=2020, compare_with_year=2019, data_set=verkehrsdaten_lowess
)

verkehrsdaten_trend.to_feather(str(data_dir / "verkehrsdaten_trend.feather"))
