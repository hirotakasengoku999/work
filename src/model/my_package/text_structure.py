# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import re
import collections
import pandas as pd

#%%
def text_separete_by_items(text, items, pattern, other_label='その他', rtype='Series'):
    re_com = re.compile(pattern)
    item_dict = {}
    for item in items:
        item_dict[item] = ''
    separaters = re_com.findall(text)
    seped_texts = re_com.split(text)
    
    item_dict[other_label] = seped_texts[0]
    for i, separater in enumerate(separaters):
        # 構造化チェック
        if separater in items:
            item_dict[separater] = item_dict[separater] + seped_texts[i + 1]
        else:
            item_dict[other_label] = item_dict[other_label] + separater + seped_texts[i + 1]

    # returnチェック
    if rtype == 'Series':
        se_item = pd.Series(list(item_dict.values()), index=list(item_dict.keys()))
        return se_item
    else:
        return item_dict