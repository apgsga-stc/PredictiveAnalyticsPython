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

################################################################################
# # Model Validation

# %% Plot: Feature importance
def rforest_features_report(model, features_col):
    # from operator import itemgetter
    feature_importance_df = (
        pd.DataFrame(
            {
                "Features": features_col,
                "Importance": model.feature_importances_,
            }
        )
        .sort_values("Importance", ascending=False)
        .reset_index()
        .drop(columns=["index"])
        .head(50)
    )
    feature_importance_df.loc[:,"CumSum"] = feature_importance_df.Importance.cumsum()

    print(feature_importance_df)


################################################################################
# ## Confusion Matrix


def confusion_matrices(x_test, y_test, model):

    pred_forest_01 = model.predict(x_test)

    confusion_forest_01 = confusion_matrix(y_test, pred_forest_01)

    df_confusion_forest_01 = pd.DataFrame(
        confusion_forest_01, index=["Fact 0", "Fact 1"], columns=["Pred 0", "Pred 1"]
    )

    print("Test set balance:")
    print(pd.Series(y_test).value_counts())

    print("\nConfusion Matrices:")

    print("\nRandom Forest (forest_01):")
    print(df_confusion_forest_01)


################################################################################
# ## Values: Precision, Recall, Threshold


def prec_rec_values(x_test, y_test, model):

    # RandomForestClassifier has predict_proba, but not decision_function
    (
        precision_forest_01,
        recall_forest_01,
        thresholds_forest_01,
    ) = precision_recall_curve(y_test, model.predict_proba(x_test)[:, 1])
    return precision_forest_01, recall_forest_01, thresholds_forest_01


################################################################################
# ## Precision-Recall Curve


def prec_rec_curve(x_train, y_train, model):

    (precision_forest_01, recall_forest_01, thresholds_forest_01) = prec_rec_values(
        x_train, y_train, model
    )

    plt.figure(figsize=(15, 12))
    plt.grid()

    def optimum_point(precision_model, recall_model, thresholds_model, name, dot):
        optimum_idx = pd.Series.idxmin(
            pd.Series(precision_model).apply(lambda x: np.power(1 - x, 2))
            + pd.Series(recall_model).apply(lambda x: np.power(1 - x, 2))
        )

        return plt.plot(
            precision_model[optimum_idx],
            recall_model[optimum_idx],
            dot,
            markersize=10,
            label=f"{name}: threshold {thresholds_model[optimum_idx]}",
            fillstyle="none",
            c="k",
            mew=2,
        )

    ## Apply optium_point():

    # Optimum: Forest
    optimum_point(
        precision_forest_01,
        recall_forest_01,
        thresholds_forest_01,
        name="forest_01",
        dot="o",
    )

    # Prec-Rec Curve: Forest
    plt.plot(precision_forest_01, recall_forest_01, label="Random Forest")

    plt.xlabel("Precision")
    plt.ylabel("Recall")
    plt.legend(loc="best")

    plt.show()


################################################################################
# ## Receiver Operating Characteristics (ROC) and AUC


def roc_curve_graph(x_test, y_test, model):

    (fpr_forest_01, tpr_forest_01, thresholds_forest_01) = roc_curve(
        y_test, model.predict_proba(x_test)[:, 1]
    )

    def threshold_dot_50perc(fpr_model, tpr_model, thresholds_model, name, dot):
        close_default_index_forest_01 = pd.Series.idxmin(
            pd.Series(tpr_model).apply(lambda x: np.power(1 - x, 2))
            + np.power(pd.Series(fpr_model), 2)
        )

        return plt.plot(
            fpr_model[close_default_index_forest_01],
            tpr_model[close_default_index_forest_01],
            dot,
            markersize=10,
            label=f"{name} threshold: {thresholds_model[close_default_index_forest_01]}",
            fillstyle="none",
            c="k",
            mew=2,
        )

    plt.figure(figsize=(15, 12))
    plt.grid()

    plt.plot(fpr_forest_01, fpr_forest_01, linestyle="dotted", label="base line")

    plt.plot(fpr_forest_01, tpr_forest_01, label="forest_01")

    plt.xlabel("False-Postive Rate (FPR)")
    plt.ylabel("True-Positive Rate (TPR) aka. Recall")

    # find threshold closest to zero
    threshold_dot_50perc(
        fpr_forest_01, tpr_forest_01, thresholds_forest_01, "forest_01", dot="^"
    )

    plt.legend(loc=4)
    plt.show()


################################################################################
# ## Value: Area Under the Curve


def roc_auc(x_test, y_test, model):
    forest_01_auc = roc_auc_score(y_test, model.predict_proba(x_test)[:, 1])

    info("AUC for forest_01:    {:.3f}".format(forest_01_auc))


################################################################################
# End of file.
