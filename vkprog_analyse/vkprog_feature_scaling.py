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


import pandas as pd
import numpy as np

################################################################################

def scaling_crm_add2master(master_df, crm_df, features_crm):
    container_df = pd.merge(master_df, crm_df, how="left", on="Endkunde_NR")

    for col_name in list(
        np.compress(["RY" == x[0:2] for x in features_crm], features_crm)
    ):
        container_df.loc[:, col_name] = container_df.loc[:, col_name].fillna(0)
        max_ = np.nanmax(container_df.loc[:, col_name])
        min_ = np.nanmin(container_df.loc[:, col_name])

        if min_ == max_:
            container_df.loc[:, col_name] = 0
        else:
            container_df.loc[:, col_name] = (container_df.loc[:, col_name] - min_) / (
                max_ - min_
            )

    for col_name in list(
        np.compress(["Letzter" == x[0:7] for x in features_crm], features_crm)
    ):
        max_ = np.nanmax(container_df.loc[:, col_name])
        # -> those who have never been contacted
        #    will be put together with the max-ones.
        container_df.loc[:, col_name] = container_df.loc[:, col_name].fillna(
            max_
        )  # No more NaNs!
        min_ = np.nanmin(container_df.loc[:, col_name])

        if max_ == min_:
            container_df.loc[:, col_name] = 1
        else:
            container_df.loc[:, col_name] = container_df.loc[:, col_name] / max_
            # scaling, doesn't need 0
    return container_df


################################################################################
