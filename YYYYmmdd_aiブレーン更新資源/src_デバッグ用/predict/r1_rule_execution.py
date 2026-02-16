# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/23

from src.predict.f5_individual_correspondence.f_rule.s1_new_patient_extraction import call as s1  # 1
from src.predict.f5_individual_correspondence.f_rule.s4_after_hours_addition import call as s4  # 4
from src.predict.f5_individual_correspondence.f_rule.s5_midnight_addition import call as s5  # 5
from src.predict.f5_individual_correspondence.f_rule.s6_holiday_addition import call as s6  # 6
from src.predict.f5_individual_correspondence.f_rule.s7_rule_output import call as s7  # 7


def r1_main(config, logger, karte_path, heap_path, out_path, in_targets):

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["r1_rule_execution"]))

    # 処理は昇順に実行する
    for key, params in sorted(config.items()):
        try:
            if key == "1":
                s1(params, logger, heap_path, out_path, in_targets, karte_path)
            elif key == "4":
                s4(params, logger, karte_path, in_targets)
            elif key == "5":
                s5(params, logger, karte_path, in_targets)
            elif key == "6":
                s6(params, logger, karte_path, in_targets)
            elif key == "7":
                s7(params, logger, out_path, in_targets)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["r1_rule_execution"]))
