# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))
import pandas as pd
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import os


########################################################################################
data_dir = Path.home() / "data" / "mobility_dashboard"
data_dir_intervista = data_dir / "intervista"
url_intervista = "https://www.intervista.ch/media/2020/03/Download_Mobilit%C3%A4ts-Monitoring_Covid-19.zip"

########################################################################################
with urlopen(url_intervista) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(data_dir_intervista)
    list_files_intervista = os.listdir(data_dir_intervista)

########################################################################################
def rename_auspraeg(auspraeg_x: str) -> str:
    if auspraeg_x == "weniger als 2 Km":
        return "000 - 002 km"
    elif auspraeg_x == "2 - 10 Kilometer":
        return "002 - 010 km"
    elif auspraeg_x == "10 - 20 Kilometer":
        return "010 - 020 km"
    elif auspraeg_x == "20 - 50 Kilometer":
        return "020 - 050 km"
    elif auspraeg_x == "50 - 100 Kilometer":
        return "050 - 100 km"
    elif auspraeg_x == "100++ Kilometer":
        return "100++ km"
    elif auspraeg_x == "weniger als 500 Meter":
        return "000 - 000.5 km"
    elif auspraeg_x == "500 Meter - 2 Kilometer":
        return "000.5 - 002 km"
    else:
        return "error"


########################################################################################
def stacking_mobility_data(
    percentage_by_distance_categories: pd.DataFrame,
) -> pd.DataFrame:
    percent_data_cols = [
        "Total",
        "Kanton_Zürich_Ja",
        "Kanton_Zürich_Nein",
        "Alter_15-29",
        "Alter_30-64",
        "Alter_65-79",
        "Männlich",
        "Weiblich",
        "Städtisch",
        "Ländlich",
        "Erwerbstätig",
        "In_Ausbildung",
        "Nicht_Erwerbstätig",
    ]

    container_df = pd.DataFrame()
    for percent_data in percent_data_cols:
        temp_df = percentage_by_distance_categories.loc[
            :,
            [
                "Datum",
                "Beschreibung",
                "Ausprägung",
                "dayofyear",
                "auspraegung_cleaned",
                percent_data,
            ],
        ].rename(columns={percent_data: "percent"})
        temp_df.loc[:, "social_demographic"] = percent_data

        container_df = pd.concat([container_df, temp_df])
    return container_df


########################################################################################


def stacking_mobility_data_mean_med(
    mobility_zh_cat_distance: pd.DataFrame,
) -> pd.DataFrame:
    percent_data_cols = [
        "Total",
        "Kanton_Zürich_Ja",
        "Kanton_Zürich_Nein",
        "Alter_15-29",
        "Alter_30-64",
        "Alter_65-79",
        "Männlich",
        "Weiblich",
        "Städtisch",
        "Ländlich",
        "Erwerbstätig",
        "In_Ausbildung",
        "Nicht_Erwerbstätig",
    ]
    container_df = pd.DataFrame()
    for percent_data in percent_data_cols:
        temp_df = mobility_zh_cat_distance.loc[
            :, ["Datum", "Beschreibung", "Typ", "dayofyear", percent_data]
        ].rename(columns={percent_data: "kilometer"})
        temp_df.loc[:, "social_demographic"] = percent_data

        container_df = pd.concat([container_df, temp_df])
    return container_df


########################################################################################


def load_data_intervista(
    link_distanzkat, link_mean_med
) -> (pd.DataFrame, pd.DataFrame):

    # link_distanzkat = "C:/Users/stc/data/covid19/download_mobilitäts-monitoring_covid-19/Distanzkategorien_in_Prozent_pro_Tag.csv"
    # link_mean_med = "C:/Users/stc/data/covid19/download_mobilitäts-monitoring_covid-19/Mittelwerte_und_Median_pro_Tag.csv"
    # fetch data
    mobility_zh_cat_distance = pd.read_csv(
        link_distanzkat, encoding="iso8859_15"
    ).astype({"Datum": "datetime64[ns]"})
    mobility_zh_mean_med = pd.read_csv(link_mean_med, encoding="iso8859_15").astype(
        {"Datum": "datetime64[ns]"}
    )
    # data janitoring
    mobility_zh_cat_distance.loc[
        :, "dayofyear"
    ] = mobility_zh_cat_distance.Datum.dt.dayofyear
    mobility_zh_mean_med.loc[:, "dayofyear"] = mobility_zh_mean_med.Datum.dt.dayofyear
    mobility_zh_cat_distance.loc[
        :, "auspraegung_cleaned"
    ] = mobility_zh_cat_distance.Ausprägung.apply(lambda x: rename_auspraeg(x))

    mobility_cat_dist_stacked = stacking_mobility_data(
        percentage_by_distance_categories=mobility_zh_cat_distance
    )

    mobility_dist_mean_med_stacked = stacking_mobility_data_mean_med(
        mobility_zh_cat_distance=mobility_zh_mean_med
    )

    return mobility_cat_dist_stacked, mobility_dist_mean_med_stacked


########################################################################################


def master_intervista_loader():
    distanz_kat_prozent = [
        x for x in list_files_intervista if "distanzkategorien_in_prozent" in x.lower()
    ][0]

    mittelwerte_mediane = [
        x for x in list_files_intervista if "mittelwert" in x.lower()
    ][0]

    dist_cat_df, dist_mean_med = load_data_intervista(
        link_distanzkat=data_dir_intervista / distanz_kat_prozent,
        link_mean_med=data_dir_intervista / mittelwerte_mediane,
    )
    return dist_cat_df, dist_mean_med


########################################################################################
# dist_cat_df, dist_mean_med = master_intervista_loader()
