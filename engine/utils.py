import pandas as pd


def get_list_of_dict(data):
    list = []
    data = data.reset_index()
    for index, row in data.iterrows():
        list.append(row.to_dict())
    return list


def get_df_from_list_of_dict(list):
    return pd.DataFrame.from_dict(list)
