# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import json

def read_config(config_path):
    """
    個別設定ファイルの読み込み
    Parameters
    ----------
    config_path: str
        個別設定ファイルのパス
    Returns
    -------
        dict
            個別設定ファイルの値
    """

    try:
        _config = {}
        with open(config_path, "r", encoding="utf-8") as f:
            _config = json.load(f)
    except:
        raise
    return _config
