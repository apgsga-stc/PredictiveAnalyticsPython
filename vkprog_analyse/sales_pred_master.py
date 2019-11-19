#!/usr/bin/env python
# coding: utf-8

################################################################################
# # Master File

################################################################################
# # Tools & Libraries
################################################################################

# make imports from pa_lib possible (parent directory of file's directory)

import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))

from pa_lib.log import time_log, info

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pa_lib.data import (boxplot_histogram)

from pa_lib.file import (
    project_dir,
    load_bin,
    load_csv,
    load_xlsx,
    store_bin
    )

################################################################################
info("sales_pred_master.py: START")
################################################################################
# Global Variables

# Output: Name of scored list (saved in data/vkprog/predictions)
ek_list_name = "20191202_ek_list_testing.feather"


# date for prediction:
day_predict   = 2 # Make sure it's a Monday
month_predict = 12
year_predict  = 2019


# Year for training the model (Random Forest) on:
year_training = 2018


################################################################################
# Lazy Recursive Job Dependency Request:
from pa_lib.job import request_job

################################################################################
## Recursive Dependency Check:

request_job(
    job_name= "ek_info_prepare.py",
    #current = "This Week",
    current = "Today"
    ) 
# output: ek_info.feather

################################################################################
# # Load Dataset (Data Preparation)
################################################################################

from vkprog_data_prep import bd_train_scoring

# 2019-10-21 => Calendar week 43
(training_all,
 scoring_all,
 feature_colnames_bd,
 feature_colnames_dates,
 feature_colnames_branchen
    ) = bd_train_scoring(
            day            = day_predict, # a Monday
            month          = month_predict,
            year_score     = year_predict,
            year_train     = year_training,
            year_span      = 4,  # we take the last four years into account
            scale_features = True,
            sales_filter   = True
            #Sales Filter: Keine Langzeitverträge, Eigenwerbung, 
            #              Logistik für Dritte, politisch... etc.
            )

################################################################################
# ## CRM Data

from vkprog_crm_prep  import crm_train_scoring

(crm_train_df,
 crm_score_df,
 feature_colnames_crm
) = crm_train_scoring(
    day        = day_predict,
    month      = month_predict,
    year_score = year_predict,
    year_train = year_training,
    year_span  = 4 # we take the last four years into account
    )

################################################################################

def scaling_crm_add2master(master_df,crm_df,feature_colnames_crm):
    
    container_df = pd.merge(master_df, crm_df,how="left", on="Endkunde_NR")

    for col_name in list(
        np.compress(
            ['RY'== x[0:2] for x in feature_colnames_crm],
            feature_colnames_crm
            )
        ):
        
        container_df.loc[:,col_name] = container_df.loc[:,col_name].fillna(0)
        
        max_ = np.nanmax(
            container_df.loc[:,col_name]
            )
        
        min_ = np.nanmin(
            container_df.loc[:,col_name]
            )
        
        if min_ == max_:
            container_df.loc[:,col_name] = 0
        
        else:
            container_df.loc[:,col_name] = (
                (container_df.loc[:,col_name] - min_) / (max_ - min_)
                )

    for col_name in list(
        np.compress(
            ['Letzter'== x[0:7] for x in feature_colnames_crm],
            feature_colnames_crm
            )
        ):
        
        max_ = np.nanmax(container_df.loc[:,col_name]) 
        # -> those who have never been contacted
        #    will be put together with the max-ones.
        
        container_df.loc[:,col_name] = (
            container_df.loc[:,col_name]
                        .fillna(max_)
            ) # No more NaNs!
        
        min_ = np.nanmin(container_df.loc[:,col_name])
        
        if max_ == min_:
            container_df.loc[:,col_name] = 1
            
        else:
            container_df.loc[:,col_name] = container_df.loc[:,col_name]/max_
            # scaling, doesn't need 0
    
    return container_df

################################################################################

training_all = scaling_crm_add2master(
    master_df            = training_all,
    crm_df               = crm_train_df,
    feature_colnames_crm = feature_colnames_crm
    )


scoring_all  = scaling_crm_add2master(
    master_df            = scoring_all,
    crm_df               = crm_score_df,
    feature_colnames_crm = feature_colnames_crm
    )

################################################################################
# # Modeling
################################################################################
# ## Define Columns: Features versus Targets

from itertools import compress

features = (
    feature_colnames_bd         # Booking data
    + feature_colnames_dates    # Dates related to bookings
    + feature_colnames_branchen # Branches
    + feature_colnames_crm      # CRM (customer-vkber interactions)
    )

feature_columns_boolean = pd.Series(features).str.match('^Target')
feature_columns = pd.Series(features).loc[~feature_columns_boolean]

feature_columns_boolean = pd.Series(training_all.columns).str.match('^Target')
target_columns = pd.Series(training_all.columns).loc[feature_columns_boolean]

del feature_columns_boolean

info(f"Number of features: {len(feature_columns)}\n")
info(f"Target columns: {target_columns}")

################################################################################
# ## Split ``training_all`` into training-set (``X_train``,``y_train``) and test-set (``X_test``,``y_test``)

df_features = (
    training_all
    .loc[:,feature_columns]
    .to_numpy()
    )

df_target   = (
    training_all
    .loc[:, "Target_Res_flg"]
    .to_numpy()
    )

df_scoring_features = (
    scoring_all
    .loc[:,feature_columns]
    .to_numpy()
    )

print(f"df_features.shape: {df_features.shape}")
print(f"df_target.shape:   {df_target.shape}")

################################################################################

from sklearn.model_selection import train_test_split

(X_train, X_test, y_train, y_test) = train_test_split(
    df_features,
    df_target,
    train_size=0.75,
    random_state=42
    )


info(f"X_train.shape: {X_train.shape}")
info(f"y_train.shape: {y_train.shape}")
info(f"X_test.shape.: {X_test.shape}")
info(f"y_test.shape:  {y_test.shape}")
info(f"df_scoring_features.shape: {df_scoring_features.shape}")

################################################################################

from scipy import stats

print('y_train:')
print(pd.DataFrame(y_train).groupby(0)[0].count())
print(stats.describe(y_train))

print('\ny_test:')
print(pd.DataFrame(y_test).groupby(0)[0].count())
print(list(stats.describe(y_test)))

################################################################################
# ## Balance Training Dataset


from imblearn.over_sampling import SMOTE

sm  = SMOTE(random_state=42)

(X_train_balanced, y_train_balanced) = sm.fit_resample(X_train, y_train)


print('y_train_balanced:')
print(pd.DataFrame(y_train_balanced).groupby(0)[0].count())
print(stats.describe(y_train_balanced))

################################################################################
# ## Feature selection: SelectkBest

from sklearn.feature_selection import (
    SelectKBest,
    f_classif,
    mutual_info_classif,
    #SelectPercentile
    )

from itertools import compress

################################################################################

select = SelectKBest(
    score_func = mutual_info_classif,
    k          = 150 # How many features? (currently 219 is max)
    )

select.fit(
    X_train_balanced,
    y_train_balanced
    )

mask = select.get_support() # boolean array.

info(f"X_train_balanced.shape: {X_train_balanced.shape}")
info(f"X_train_balanced[:,mask].shape: {X_train_balanced[:,mask].shape}")

################################################################################

# Reassign variable names due to lazyness
feature_columns  = feature_columns.loc[mask]
X_train_balanced = X_train_balanced[:,mask]
X_train          = X_train[:,mask]
X_test           = X_test[:,mask]
X_scoring        = df_scoring_features[:,mask]

print("X_scoring.shape:",X_scoring.shape)

################################################################################
# ## Model Training

################################################################################
# ### Model Training: Random Forest

from sklearn.ensemble import RandomForestClassifier

# Wall time: 13min
forest_01 = RandomForestClassifier(
    n_estimators = 5*10**3,
    max_depth    = 10,
    criterion    = 'gini',  #criterion='gini',
    random_state = 42,
    n_jobs       = -1
    )

forest_01.fit(
    X_train_balanced,
    y_train_balanced
    )

# %% Validate Accuracy
info(f"Accuracy on balanced training set:   {forest_01.score(X_train_balanced, y_train_balanced)}"[:42])
info(f"Accuracy on unbalanced training set: {forest_01.score(X_train,          y_train)}"[:42])
info(f"Accuracy on test set (validation):   {forest_01.score(X_test,           y_test)}"[:42])

################################################################################

# %% Plot: Feature importance
def plot_feature_importances(
        model,
        feature_columns,
        figsize=(20,100)
        ):
    
    from operator import itemgetter
    
    dict_feature_importance = sorted(
        dict(
            zip(feature_columns,model.feature_importances_)
        ).items(),
        key=itemgetter(1)
        )
    
    n_features = len(feature_columns)
    
    plt.figure(figsize=figsize)
    plt.grid()
    
    plt.barh(
        np.arange(n_features),
        [y for (x,y) in dict_feature_importance],
        align='center'
        )
    
    plt.yticks(
        np.arange(n_features),
        [x for (x,y) in dict_feature_importance]
        )
    
    plt.xlabel("Feature importance")
    plt.ylabel("Feature")
    plt.ylim(-1, n_features)
    plt.show()

################################################################################

plot_feature_importances(forest_01,feature_columns)

################################################################################
# # Model Validation
################################################################################

################################################################################
# ## Confusion Matrix

from sklearn.metrics import confusion_matrix

################################################################################

def confusion_matrices(X_test,y_test):
    global pred_forest_01
    
    pred_forest_01    = forest_01.predict(X_test)
 


    # Wall time: 20.9ms

    confusion_forest_01 = (
        confusion_matrix(
            y_test,
            pred_forest_01
            )
        )
    
    df_confusion_forest_01 = (
        pd.DataFrame(
            confusion_forest_01,
            index=["Fact 0", "Fact 1"],
            columns=["Pred 0","Pred 1"]
            )
        )

    print("Test set balance:")
    print(pd.Series(y_test).value_counts())

    print("\nConfusion Matrices:")

    print("\nRandom Forest (forest_01):")
    print(df_confusion_forest_01)

################################################################################

confusion_matrices(
    X_test = X_train_balanced,
    y_test = y_train_balanced
    )

confusion_matrices(
    X_test = X_train,
    y_test = y_train
    )

confusion_matrices(
    X_test = X_test,
    y_test = y_test
    )

################################################################################
# ## Classification Report

from sklearn.metrics import classification_report

print("Random Forest:")
print(
    classification_report(
        y_test,
        pred_forest_01,
        target_names=["not booking = 0", "booking = 1"]
        )
    )

################################################################################
# ## Precision-Recall Curve

from sklearn.metrics import precision_recall_curve

def prec_rec_values(X_test,y_test):
    global precision_forest_01,    recall_forest_01,    thresholds_forest_01
    
    # RandomForestClassifier has predict_proba, but not decision_function
    (precision_forest_01, recall_forest_01, thresholds_forest_01) = (
        precision_recall_curve(
            y_test,
            forest_01.predict_proba(X_test)[:, 1]
            )
        )
    
################################################################################

def prec_rec_curve(X_train,y_train):
    prec_rec_values(X_train,y_train)

    plt.figure(figsize=(15,12))
    plt.grid()

    def optimum_point(precision_forest_01,
                      recall_forest_01,
                      thresholds_forest_01,
                      name,
                      dot):
        
        optimum_idx = (
            pd.Series.idxmin(
                np.power(1-pd.Series(precision_forest_01),2)
                + np.power(1-pd.Series(recall_forest_01),2)
                )
            )
        
        return plt.plot(precision_forest_01[optimum_idx],
                     recall_forest_01[optimum_idx],
                     dot,
                     markersize = 10,
                     label      = f"{name}: threshold {thresholds_forest_01[optimum_idx]}",
                     fillstyle  = "none",
                     c          = 'k',
                     mew        = 2
                     )

    
    ## Apply optium_point():
    
    # Optimum: Forest 
    optimum_point(precision_forest_01,
                  recall_forest_01,
                  thresholds_forest_01,
                  name="forest_01",
                  dot='o'
                 )

    # Prec-Rec Curve: Forest
    plt.plot(precision_forest_01,
             recall_forest_01,
             label="Random Forest"
            )

    plt.xlabel("Precision")
    plt.ylabel("Recall")
    plt.legend(loc="best")

    plt.show()

################################################################################

prec_rec_curve(
    X_train = X_train_balanced,
    y_train = y_train_balanced
    )

prec_rec_curve(
    X_train = X_train,
    y_train = y_train
    )

prec_rec_curve(
    X_train = X_test,
    y_train = y_test
    )

################################################################################

from sklearn.metrics import average_precision_score 

avg_precision_forest_01 = (
    average_precision_score(
        y_test,
        forest_01.predict_proba(X_test)[:, 1]
        )
    )

info(f"Average Precision of forest_01: {avg_precision_forest_01}"[:37])

################################################################################
# ## Receiver Operating Characteristics (ROC) and AUC

from sklearn.metrics import roc_curve

def roc_curve_graph(X_test,y_test):
    global fpr_forest_01, tpr_forest_01, thresholds_forest_01
    
    (fpr_forest_01, tpr_forest_01, thresholds_forest_01) = (
        roc_curve(
            y_test,
            forest_01.predict_proba(X_test)[:, 1]
            )
        )

    def threshold_dot_50perc(fpr_forest_01,
                             tpr_forest_01,
                             thresholds_forest_01,
                             name,
                             dot):

        close_default_index_forest_01 = (
            pd.Series.idxmin(
                np.power(1-pd.Series(tpr_forest_01),2)
                +np.power(pd.Series(fpr_forest_01),2)
                )
            )
        
        return plt.plot(
            fpr_forest_01[close_default_index_forest_01],
            tpr_forest_01[close_default_index_forest_01],
            dot,
            markersize=10,
            label=f"{name} threshold: {thresholds_forest_01[close_default_index_forest_01]}",
            fillstyle="none",
            c='k',
            mew=2)


    plt.figure(figsize=(15,12))
    plt.grid()

    plt.plot(fpr_forest_01,
             fpr_forest_01,
             linestyle='dotted',
             label="base line"
            )

    plt.plot(fpr_forest_01,
             tpr_forest_01,
             label="forest_01"
            )

    plt.xlabel("False-Postive Rate (FPR)")
    plt.ylabel("True-Positive Rate (TPR) aka. Recall")

    # find threshold closest to zero
    threshold_dot_50perc(fpr_forest_01,
                         tpr_forest_01,
                         thresholds_forest_01,
                         'forest_01',
                         dot='^'
                        )

    plt.legend(loc=4)
    plt.show()

################################################################################

from sklearn.metrics import roc_auc_score

def roc_auc(X_test,y_test):
    forest_01_auc = roc_auc_score(
        y_test,
        forest_01.predict_proba(X_test)[:, 1]
        )
    
    info("AUC for forest_01:    {:.3f}".format(forest_01_auc))

################################################################################

#Wall time: 10.6 s
roc_curve_graph(
    X_train_balanced,
    y_train_balanced
    )

roc_auc(
    X_train_balanced,
    y_train_balanced
    )
#

roc_curve_graph(
    X_train,
    y_train
    )
#

roc_auc(
    X_train,
    y_train
    )
#

roc_curve_graph(
    X_test,
    y_test
    )

roc_auc(
    X_test,
    y_test
    )

################################################################################
# # Scoring
################################################################################
# ## Score Class Probabilities (Booking: No/Yes)

scoring_prob     = forest_01.predict_proba(X_scoring)

scoring_prob_df  = pd.DataFrame(scoring_prob,
                                columns=["Prob_0","Prob_1"]
                               )

scoring_all_prob = (pd.merge(scoring_all,
                             scoring_prob_df,
                             left_index=True,
                             right_index=True
                            ).sort_values("Prob_1",ascending=False)
                   )

################################################################################
# ## Adding additional information for delivery lists ``EK_LIST_2W_KOMPLETT.csv``

from pa_lib.file import load_bin

ek_info = load_bin("vkprog\\ek_info.feather")

################################################################################

ek_list_raw = pd.merge(
    scoring_all_prob,
    ek_info,
    on="Endkunde_NR",
    how="left"
    )

################################################################################

net_columns = [col for col in ek_info.columns if col.startswith("Net_")]

col_row_filter =([
    "Insolvenz",
    #"Last_Res_Date",  # covered in listing
    #"Last_Aus_Date",  # covered in listing
    "last_CRM_Ktkt_date",
    "VB_FILTER_VON",
    "VB_FILTER_BIS"
    ])

listing = (
    ["Endkunde_NR",      # Endkunde_NR
     "Endkunde",         # Endkunde
     "EK_HB_Apg_Kurzz",  # HB_APG (based on R-script)
     "Agentur",          # Agentur
     "AG_Hauptbetreuer", # HB_Agentur   
     "PLZ",              # PLZ
     "GEMEINDE"]        # Ort
    
     +net_columns       # Net_2015, Net_2016, Net_2017, Net_2018, Net_2019 
    
     +["letzte_VBs",     # (bd, aggregiert)
       "Letzter_Kontakt", # KZ_letzter_Ktkt (crm)
       "Kanal",           #(crm)
       "Betreff",         #(crm)    
       "Last_Res_Date", # Letzte_Kamp_erfasst
       "Last_Aus_Date", # letzte_Kamp_Beginn
       "VERKAUFS_GEBIETS_CODE", # Verkaufsgebiet
       "VB_VKGEB",      
       "Prob_1"        # prob_KW (from here, good good.)
       ]
    # Needed for row_filter
    + col_row_filter
    )

ek_list = (ek_list_raw
    .loc[:,listing]
    .rename(columns={
        "EK_HB_Apg_Kurzz": "HB_APG",
        "AG_Hauptbetreuer": "HB_Agentur",
        "GEMEINDE": "Ort",
        "Letzter_Kontakt": "letzter_Kontakt",
        "Last_Res_Date": "letzte_Kamp_erfasst",
        "Last_Aus_Date":"letzte_Kamp_Beginn",
        "VERKAUFS_GEBIETS_CODE": "Verkaufsgebiet",
        "VB_VKGEB": "VB_VK_Geb",
        "Prob_1": "prob_KW"
    })       
    )

info(f"ek_list.shape: {ek_list.shape}")

################################################################################
# ## Deployment for ``vp2xlsx.py``

with project_dir("vkprog\\predictions"):
    store_bin(
        ek_list,
        ek_list_name # Output name.
        )

################################################################################
# End of file.
info("Continue with: vp2xlsx_rebuild.py")
info("sales_pred_master.py: END")
################################################################################
