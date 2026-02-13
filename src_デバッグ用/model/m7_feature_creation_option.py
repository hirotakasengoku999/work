# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

from src.model.f6_individual_correspondence_merging.s_merging_option import call as merging_option  # 1
from src.model.f6_individual_correspondence_merging.s_calculations_count import call as calculations_count  # 6
from src.model.f6_individual_correspondence_merging.s_special_meal_flag import call as special_meal  # 11


def m7_main(config, logger, model_path, model_flag, in_targets=[]):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m7_feature_creation_option"]))
    # 処理は昇順に実行する
    # for key, params in sorted(config.items()):
    for key, params in config.items():
        try:
            if key == "1":  # 患者年齢
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "2":  # jcs
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "3":  # 公費法別番号
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "4":  # 悪性腫瘍
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "5":  # 予定緊急入院区分
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "6":
                calculations_count(params, logger, model_path, model_flag, in_targets)
            elif key == "7":  # 入院日数
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "8":  # トリアージレベル
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "9":  # 検査項目
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "10":  # 介護支援
                merging_option(params, logger, key, model_path, model_flag, in_targets)
            elif key == "11":
                special_meal(params, logger, model_path, model_flag, in_targets)
            elif key == "_comment":
                pass
            else:
                # 固有処理の呼び出し
                exec(f"import {params['package']}; {params['package']}.{params['function']}({','.join(params['params'])})")
        except TypeError as te:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003001", te.args))
            raise
        except OSError as ose:
            import traceback, sys
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003002", [ose.filename, traceback.format_exception(*sys.exc_info())[-1]]))
            raise
        except KeyError as ke:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ke.args))
            raise
        except:
            l = lambda x:x[0](x[1]); l(logger.log_message("AIR003006"))
            raise
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m7_feature_creation_option"]))
