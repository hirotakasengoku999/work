# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
from . import text_analogy as ta

#%%
def process_pattern3(text):
    """
    アルファベット、カタカナを全角統一
    ↓
    アルファベッドを小文字に統一
    ↓
    数字を半角に統一
    ↓
    "/"を全角に統一
    """
    
    # アルファベッド、カタカナを全角統一
    text = ta.han2zen_KaAl(text)
    # アルファベッドを小文字に統一
    text = ta.large2small(text)
    # 数字を半角に統一
    text = ta.zen2han_Num(text)
    # "/"を全角に統一
    text = text.replace('/', '／')
    # ハイフンを統一
    text = text.replace('−', '－').replace('-', '－')
    # "("を半角に統一
    text = text.replace('（', '(').replace('）', ')')

    return text
