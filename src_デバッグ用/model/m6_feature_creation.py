# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

from src.model.f7_randomforest.s1_feature_creation import call as feature_creation1  # 1
from src.model.f7_randomforest.s1_feature_creation_surgical_anesthesia import call as feature_creation2  # 2


def m6_main(config, logger, model_path, model_flag, in_targets=[]):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m6_feature_creation"]))
    # 処理は昇順に実行する
    for key, params in sorted(config.items()):
        try:
            if key == "1":
                feature_creation1(params, logger, model_path, model_flag, in_targets)
            elif key == "2":
                feature_creation2(params, logger, model_path, model_flag, in_targets)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m6_feature_creation"]))
