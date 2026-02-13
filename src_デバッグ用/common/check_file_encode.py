# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import os
import glob


def check_files(dir_path):
    """
    指定されたディレクトリパスに存在する全てのファイルを開き、
    文字エンコードが Shift_JISx0213 でエンコードできるか確認します。
    エンコードの確認は行単位で実施され、エンコードエラーを発見した場合、
    ファイル名とエラーとなった行数を返却します。

    :param dir_path:
    :return: list[(ファイル名,エラーとなった行数)]
    """
    if not os.path.isdir(dir_path):
        raise Exception(dir_path + " is not directory")

    file_list = [os.path.abspath(p) for p in glob.glob(os.path.abspath(dir_path)+"/**/*", recursive = True) if os.path.isfile(p)]

    failed_list = []
    # ファイルの数分ループ
    for file_path in file_list:
        try:
            if os.path.isfile(file_path):
                with open(file_path, mode='rb') as f:
                    # バイナリでオープンしたファイルを行ごとに読み込んでエンコードしていく
                    for idx, text in enumerate(f.readlines()):
                        try:
                            text.decode(encoding='Shift_JISx0213')
                        except:
                            failed_list.append((file_path, idx + 1))
        except:
            failed_list.append((file_path, 0))

    # エラーになった文字を削除して保存しなおす
    failed_file_list = set([file[0] for file in failed_list])
    for failed_file in failed_file_list:
        with open(failed_file, mode="r", encoding='Shift_JISx0213', errors='ignore') as rf:
            buff_string = "".join(rf.readlines())
        with open(failed_file, mode="w", encoding='Shift_JISx0213', errors='ignore') as wf:
            wf.write(buff_string)

    return failed_list
