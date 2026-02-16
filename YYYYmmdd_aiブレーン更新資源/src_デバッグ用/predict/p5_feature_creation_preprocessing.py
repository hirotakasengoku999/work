# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import traceback, sys
from src.model.f5_individual_correspondence.f_emergency_medical_addition.s_by_category import call as t1  # 1
from src.model.f5_individual_correspondence.f_general_anesthesia_4.s_by_category import call as t2  # 2


def p5_main(config, logger, predict_flag, in_targets):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["p5_feature_creation_preprocessing"]))
    # 処理は昇順に実行する
    for key, params in config.items():
        try:
            if key == "1":
                t1(params, logger, predict_flag, in_targets)
            elif key == "2":
                t2(params, logger, predict_flag, in_targets)
            elif key == "_comment":
                pass
            else:
                # 固有処理の呼び出し
                # l = lambda x:x[0](x[1]); l(logger.info("not implemented yet.:{0}" + key )
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["p5_feature_creation_preprocessing"]))
