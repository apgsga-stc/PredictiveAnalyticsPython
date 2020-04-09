# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))
import os.path

from datetime import timedelta, date
import pandas as pd
import statsmodels.api as sm

data_dir = Path.home() / "data" / "dashboard-corona"

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
def load_verkehr_data(url_verkehr) -> pd.DataFrame:
    zh_raw_df = pd.read_csv(url_verkehr, low_memory=False)
    zh_raw_df = zh_raw_df.astype(
        {"MessungDatZeit": "datetime64[ns]", "LieferDat": "datetime64[ns]"}
    )
    msid_count = len(list(set(zh_raw_df.MSID)))
    number_rows = zh_raw_df.shape[0]

    print(f"LieferDat:{zh_raw_df.LieferDat.max()}")
    print(f"MessungDatZeit:{zh_raw_df.MessungDatZeit.min()}")
    print(f"MessungDatZeit:{zh_raw_df.MessungDatZeit.max()}")
    print(f"No. of MSID: {msid_count}")

    lowess = sm.nonparametric.lowess
    lowess_anzfahrzeuge = lowess(
        exog=zh_raw_df.MessungDatZeit,
        endog=zh_raw_df.AnzFahrzeuge,
        frac=9 * (msid_count * 24) / number_rows,  # 0.0075 * 3.25,
        return_sorted=False,
        missing="drop",
    )
    zh_raw_df.loc[:, "anzfahrzeuge_lowess"] = lowess_anzfahrzeuge
    zh_verkehr = (
        zh_raw_df.loc[:, ["MessungDatZeit", "anzfahrzeuge_lowess",],]
        .drop_duplicates()
        .interpolate(method="linear", axis=0)
    )

    messdates_isocal = zh_verkehr.MessungDatZeit.apply(
        lambda mess_date: mess_date.isocalendar()
    ).copy()
    zh_verkehr.loc[:, "year_iso"] = messdates_isocal.map(
        lambda x: x[0]
    )  # for separating the years in plotly
    zh_verkehr.loc[:, "KW_iso"] = messdates_isocal.map(
        lambda x: x[1]
    )  # for labeling in plotly
    zh_verkehr.loc[:, "hourofyear_iso"] = (
        messdates_isocal.map(lambda x: (x[1] - 1) * 7 * 24 + (x[2] - 1) * 24)
        + zh_verkehr.MessungDatZeit.dt.hour
    )  ## for technical x-axis in plotly.

    return zh_verkehr.reset_index().drop(columns=["index"])


########################################################################################


def prepare_verkehrsdaten():
    verkehrsdaten_container = pd.DataFrame()
    year_now = date.today().year
    for url_year in url_verkehr:
        year = int(url_year[-8:-4])
        file_name = f"verkehrsdaten_lowess_{year}.feather"
        if (os.path.exists(data_dir / file_name)) and (year != year_now):
            print(f"{file_name} already exists.")
            stored_data = pd.read_feather(data_dir / file_name)
            verkehrsdaten_container = pd.concat([verkehrsdaten_container, stored_data])
        else:
            comput_data = load_verkehr_data(url_verkehr=url_year)
            comput_data.to_feather(str(data_dir / file_name))
            verkehrsdaten_container = pd.concat([verkehrsdaten_container, comput_data])

    verkehrsdaten_lowess = (
        verkehrsdaten_container.sort_values("MessungDatZeit")
        .reset_index()
        .drop(columns=["index"])
    )
    verkehrsdaten_lowess.to_feather(str(data_dir / "verkehrsdaten_lowess.feather"))

    return verkehrsdaten_lowess


test = prepare_verkehrsdaten()
########################################################################################


import seaborn as sns

sns.lineplot(
    y="anzfahrzeuge_lowess",
    x="hourofyear_iso",
    hue="year_iso",
    estimator=None,
    data=test,
)
