#!/usr/bin/env python
# coding: utf-8

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

from pa_lib.log import info

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import (
    confusion_matrix,
    precision_recall_curve,
    roc_curve,
    roc_auc_score,
)

from operator import itemgetter

################################################################################
# # Model Validation

################################################################################

# %% Plot: Feature importance
def plot_rforest_features(model, features_col, figsize=(20, 150)):
    #from operator import itemgetter

    dict_feature_importance = sorted(
        dict(zip(features_col, model.feature_importances_)).items(), key=itemgetter(1)
    )

    n_features = len(features_col)

    plt.figure(figsize=figsize)
    plt.grid()

    plt.barh(
        np.arange(n_features), [y for (x, y) in dict_feature_importance], align="center"
    )

    plt.yticks(np.arange(n_features), [x for (x, y) in dict_feature_importance])

    plt.xlabel("Feature importance")
    plt.ylabel("Feature")
    plt.ylim(-1, n_features)
    plt.show()



################################################################################
# ## Confusion Matrix

def confusion_matrices(x_test, y_test, model):
    #global pred_forest_01

    pred_forest_01 = model.predict(x_test)

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
            columns=["Pred 0", "Pred 1"]
        )
    )

    print("Test set balance:")
    print(pd.Series(y_test).value_counts())

    print("\nConfusion Matrices:")

    print("\nRandom Forest (forest_01):")
    print(df_confusion_forest_01)

################################################################################
# ## Precision-Recall Curve

def prec_rec_values(X_test, y_test,model):
    global precision_forest_01, recall_forest_01, thresholds_forest_01

    # RandomForestClassifier has predict_proba, but not decision_function
    (precision_forest_01, recall_forest_01, thresholds_forest_01) = (
        precision_recall_curve(
            y_test,
            model.predict_proba(X_test)[:, 1]
        )
    )


################################################################################

def prec_rec_curve(x_train, y_train, model):
    prec_rec_values(x_train, y_train, model)

    plt.figure(figsize=(15, 12))
    plt.grid()

    def optimum_point(precision_forest_01,
                      recall_forest_01,
                      thresholds_forest_01,
                      name,
                      dot):
        optimum_idx = (
            pd.Series.idxmin(
                #np.power(1 - pd.Series(precision_forest_01), 2)
                pd.Series(precision_forest_01).apply(lambda x: np.power(1-x,2))
                + pd.Series(recall_forest_01).apply(lambda x: np.power(1-x,2))
            )
        )

        return plt.plot(precision_forest_01[optimum_idx],
                        recall_forest_01[optimum_idx],
                        dot,
                        markersize=10,
                        label=f"{name}: threshold {thresholds_forest_01[optimum_idx]}",
                        fillstyle="none",
                        c='k',
                        mew=2
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
# ## Receiver Operating Characteristics (ROC) and AUC

def roc_curve_graph(x_test, y_test, model):

    (fpr_forest_01, tpr_forest_01, thresholds_forest_01) = (
        roc_curve(
            y_test,
            model.predict_proba(x_test)[:, 1]
        )
    )

    def threshold_dot_50perc(fpr_forest_01,
                             tpr_forest_01,
                             thresholds_forest_01,
                             name,
                             dot):
        close_default_index_forest_01 = (
            pd.Series.idxmin(
                pd.Series(tpr_forest_01).apply(lambda x: np.power(1-x,2))
                + np.power(pd.Series(fpr_forest_01), 2)
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

    plt.figure(figsize=(15, 12))
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

def roc_auc(x_test, y_test, model):
    forest_01_auc = roc_auc_score(
        y_test,
        model.predict_proba(x_test)[:, 1]
    )

    info("AUC for forest_01:    {:.3f}".format(forest_01_auc))

################################################################################
# End of file.
