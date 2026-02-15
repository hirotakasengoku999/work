# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/23

import traceback, sys
from src.predict.f7_randomforest.s2_randomforest_prediction import call as rf_predict  # 1
from src.predict.f7_randomforest.s3_change_prediction import call as change_predict  # 2
from src.predict.f7_randomforest.s4_calculation_basis import call as calc_basis  # 3


def p8_main(config, logger, model_path, in_targets):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["p8_predictive_execution"]))
    # 処理は昇順に実行する
    for key, params in sorted(config.items()):
        try:

            if key == "1":
                rf_predict(params, logger, model_path, in_targets)
            elif key == "2":
                change_predict(params, logger, in_targets)
            elif key == "3":
                calc_basis(params, logger, model_path, in_targets)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["p8_predictive_execution"]))
