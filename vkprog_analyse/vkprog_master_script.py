#!/usr/bin/env python
# coding: utf-8
# Master File

########################################################################################
# # Tools & Libraries
########################################################################################

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
from sklearn.model_selection import train_test_split
from scipy import stats
from imblearn.over_sampling import SMOTE
from sklearn.feature_selection import SelectKBest, mutual_info_classif
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, average_precision_score

# Utilities:
from pa_lib.file import project_dir, store_bin
from pa_lib.job import request_job
from pa_lib.file import load_bin
from pa_lib.log import info

# Special libs:
from vkprog_analyse.vkprog_dataprep_booking import bd_train_scoring
from vkprog_analyse.vkprog_dataprep_crm import crm_train_scoring
from vkprog_analyse.vkprog_feature_scaling import scaling_crm_add2master
from vkprog_analyse.vkprog_model_validation import (
    plot_rforest_features,
    roc_auc,
    prec_rec_curve,
    confusion_matrices,
    roc_curve_graph,
)

info("vkprog_master_script.py: START")

########################################################################################
# Recursive Dependency Check:
########################################################################################
info("Recursive Dependency Check")

request_job(
    job_name="ek_info_prepare.py",
    current="Today",
)

########################################################################################
# Global Variables
########################################################################################

# Output: Name of scored list (saved in data/vkprog/predictions)
ek_list_name = "20200323_ek_list.feather"

# date for prediction:
day_predict = 23  # Make sure it's a Monday
month_predict = 3
year_predict = 2020

# Year for training the model (Random Forest) on:
year_training = 2019


def do_debug() -> bool:
    return False


info(f"ek_list_name: {ek_list_name}")


########################################################################################
# Data Preparation
########################################################################################
info("Data Preparation")

if do_debug():
    print(f"start bd_train_scoring: {day_predict}, {month_predict}, {year_predict}")

## IT21 Data (booking data):

(
    training_all,
    scoring_all,
    feature_colnames_bd,
    feature_colnames_dates,
    feature_colnames_branchen,
) = bd_train_scoring(
    day=day_predict,  # a Monday
    month=month_predict,
    year_score=year_predict,
    year_train=year_training,
    year_span=4,  # we take the last four years into account
    scale_features=True,
    sales_filter=True
    # Sales Filter: Keine Langzeitverträge, Eigenwerbung,
    #              Logistik für Dritte, politisch... etc.
)

## CRM Data:

(crm_train_df, crm_score_df, feature_colnames_crm) = crm_train_scoring(
    day=day_predict,
    month=month_predict,
    year_score=year_predict,
    year_train=year_training,
    year_span=4,  # we take the last four years into account
)

## Feature Scaling:

training_all = scaling_crm_add2master(
    master_df=training_all, crm_df=crm_train_df, features_crm=feature_colnames_crm
)

scoring_all = scaling_crm_add2master(
    master_df=scoring_all, crm_df=crm_score_df, features_crm=feature_colnames_crm
)

## Define Columns: Features versus Targets:

features = (
    feature_colnames_bd  # Booking data
    + feature_colnames_dates  # Dates related to bookings
    + feature_colnames_branchen  # Branches
    + feature_colnames_crm  # CRM (customer-vkber interactions)
)

feature_columns_boolean = pd.Series(features).str.match("^Target")
feature_columns = pd.Series(features).loc[~feature_columns_boolean]

feature_columns_boolean = pd.Series(training_all.columns).str.match("^Target")
target_columns = pd.Series(training_all.columns).loc[feature_columns_boolean]

info(f"Number of features: {len(feature_columns)}\n")
info(f"Target columns: {target_columns}")

########################################################################################
# Modeling
########################################################################################
info("Modeling")

df_features = training_all.loc[:, feature_columns].to_numpy()
df_target = training_all.loc[:, "Target_Res_flg"].to_numpy()
df_scoring_features = scoring_all.loc[:, feature_columns].to_numpy()

info(f"df_features.shape: {df_features.shape}")
info(f"df_target.shape:   {df_target.shape}")
info(f"df_scoring_features.shape: {df_scoring_features.shape}")

## Splitting df_features in two sets for training and validation:
(X_train, X_test, y_train, y_test) = train_test_split(
    df_features, df_target, train_size=0.75, random_state=42
)

info(f"X_train.shape: {X_train.shape}")
info(f"y_train.shape: {y_train.shape}")
info(f"X_test.shape.: {X_test.shape}")
info(f"y_test.shape:  {y_test.shape}")
info("y_train:")
info(pd.DataFrame(y_train).groupby(0)[0].count())
info(stats.describe(y_train))
info("\ny_test:")
info(pd.DataFrame(y_test).groupby(0)[0].count())
info(list(stats.describe(y_test)))

## Balance Training Dataset:

info("SMOTE: Synthetic Minority Over-sampling Technique")

sm = SMOTE(random_state=42)
(X_train_balanced, y_train_balanced) = sm.fit_resample(X_train, y_train)

info("y_train_balanced:")
info(pd.DataFrame(y_train_balanced).groupby(0)[0].count())
info(stats.describe(y_train_balanced))

## Feature selection:

info("Feature selection")

select = SelectKBest(
    score_func=mutual_info_classif, k=150  # How many features? (currently 219 is max)
)

select.fit(X_train_balanced, y_train_balanced)
mask = select.get_support()  # boolean array.

info(f"X_train_balanced.shape: {X_train_balanced.shape}")
info(f"X_train_balanced[:,mask].shape: {X_train_balanced[:, mask].shape}")

## Reassign variable names due to lazyness:

feature_columns = feature_columns.loc[mask]
X_train_balanced = X_train_balanced[:, mask]
X_train = X_train[:, mask]
X_test = X_test[:, mask]
X_scoring = df_scoring_features[:, mask]

info(f"X_scoring.shape:{X_scoring.shape}")

########################################################################################
# Model Training: Random Forest
########################################################################################

forest_01 = RandomForestClassifier(
    n_estimators=5 * 10 ** 3,
    max_depth=10,
    criterion="gini",  # criterion='gini',
    random_state=42,
    n_jobs=-1,
)

forest_01.fit(X_train_balanced, y_train_balanced)

info(
    f"Accuracy on balanced training set:   {forest_01.score(X_train_balanced, y_train_balanced)}"[
        :42
    ]
)
info(f"Accuracy on unbalanced training set: {forest_01.score(X_train, y_train)}"[:42])
info(f"Accuracy on test set (validation):   {forest_01.score(X_test, y_test)}"[:42])

## Plot features ranked by importance:

plot_rforest_features(model=forest_01, features_col=feature_columns, figsize=(20, 150))

########################################################################################
# # Model Validation
########################################################################################
info("Model Validation")

## Confusion Matrix:

#confusion_matrices(x_test=X_train_balanced, y_test=y_train_balanced, model=forest_01)
#confusion_matrices(x_test=X_train, y_test=y_train, model=forest_01)
confusion_matrices(x_test=X_test, y_test=y_test, model=forest_01)

## Classification Report:

info("Calssification Report:")
print(
    classification_report(
        y_test,
        forest_01.predict(X_test),
        target_names=["not booking = 0", "booking = 1"],
    )
)

## Precision-Recall Curve:

#prec_rec_curve(x_train=X_train_balanced, y_train=y_train_balanced, model=forest_01)
#prec_rec_curve(x_train=X_train, y_train=y_train, model=forest_01)
prec_rec_curve(x_train=X_test, y_train=y_test, model=forest_01)

## Average Precision:

avg_precision_forest_01 = average_precision_score(
    y_test, forest_01.predict_proba(X_test)[:, 1]
)
info(f"Average Precision of forest_01: {avg_precision_forest_01}"[:37])

## Receiver Operating Characteristics (ROC) and AUC:

#roc_curve_graph(X_train_balanced, y_train_balanced, model=forest_01)
#roc_auc(X_train_balanced, y_train_balanced, forest_01)

#roc_curve_graph(X_train, y_train, model=forest_01)
#roc_auc(X_train, y_train, forest_01)

roc_curve_graph(X_test, y_test, model=forest_01)
roc_auc(X_test, y_test, forest_01)

########################################################################################
# Scoring:
########################################################################################

# Score Class Probabilities (Booking: No/Yes)

scoring_prob = forest_01.predict_proba(X_scoring)

scoring_prob_df = pd.DataFrame(scoring_prob, columns=["Prob_0", "Prob_1"])

scoring_all_prob = pd.merge(
    scoring_all, scoring_prob_df, left_index=True, right_index=True
).sort_values("Prob_1", ascending=False)

########################################################################################
# Preparation: Deployment
########################################################################################

ek_info = load_bin("vkprog/ek_info.feather")
ek_list_raw = pd.merge(scoring_all_prob, ek_info, on="Endkunde_NR", how="left")

net_columns = [col for col in ek_info.columns if col.startswith("Net_")]

col_row_filter = [
    "Insolvenz",
    # "Last_Res_Date",  # covered in listing
    # "Last_Aus_Date",  # covered in listing
    "last_CRM_Ktkt_date",
    "VB_FILTER_VON",
    "VB_FILTER_BIS",
]

relevant_cols_deploy = (
    [
        "Endkunde_NR",  # Endkunde_NR
        "Endkunde",  # Endkunde
        "EK_HB_Apg_Kurzz",  # HB_APG (based on R-script)
        "Agentur",  # Agentur
        "AG_Hauptbetreuer",  # HB_Agentur
        "PLZ",  # PLZ
        "GEMEINDE",
    ]  # Ort
    + net_columns  # Net_2015, Net_2016, Net_2017, Net_2018, Net_2019
    + [
        "letzte_VBs",  # (bd, aggregiert)
        "Datum_Letzter_Ktkt",  # letzter Ktkt (Datum, crm)
        "Letzter_Kontakt",  # KZ_letzter_Ktkt (crm)
        "Kanal",  # (crm)
        "Betreff",  # (crm)
        "Last_Res_Date",  # Letzte_Kamp_erfasst
        "Last_Aus_Date",  # letzte_Kamp_Beginn
        "VERKAUFS_GEBIETS_CODE",  # Verkaufsgebiet
        "VB_VKGEB",
        "Prob_1",  # prob_KW (from here, good good.)
    ]
    # Needed for row_filter
    + col_row_filter
)

ek_list = ek_list_raw.loc[:, relevant_cols_deploy].rename(
    columns={
        "EK_HB_Apg_Kurzz": "HB_APG",
        "AG_Hauptbetreuer": "HB_Agentur",
        "GEMEINDE": "Ort",
        "Letzter_Kontakt": "letzter_Kontakt",
        "Last_Res_Date": "letzte_Kamp_erfasst",
        "Last_Aus_Date": "letzte_Kamp_Beginn",
        "VERKAUFS_GEBIETS_CODE": "Verkaufsgebiet",
        "VB_VKGEB": "VB_VK_Geb",
        "Prob_1": "prob_KW",
    }
)

info(f"ek_list.shape: {ek_list.shape}")

################################################################################
# ## Deployment for ``vp2xlsx.py``

with project_dir("vkprog\\predictions"):
    store_bin(ek_list, ek_list_name)  # Output name.

################################################################################
# End of file.
info("Continue with: vkprog_deployment.py")
info("vkprog_master_script.py: END")
################################################################################
