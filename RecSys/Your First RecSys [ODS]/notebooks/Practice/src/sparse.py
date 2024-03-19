import pandas as pd
import numpy as np

from scipy import sparse as sp

def get_coo_matrix(df, 
                   users_mapping,
                   items_mapping,
                   user_col='user_id',
                   item_col='item_id',
                   weight_col=None):
    
    if weight_col is None:
        weights = np.ones(len(df), dtype=np.float32)
    else:
        weights = df[weight_col].astype(np.float32)

    interaction_matrix = sp.coo_matrix((
        weights, 
        (
            df[user_col].map(users_mapping.get), 
            df[item_col].map(items_mapping.get)
        )
    ))
    return interaction_matrix