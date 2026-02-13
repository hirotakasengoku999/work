# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/23

import json
import os

class LogWriter:
    """
    ログメッセージを出力する関数を実装したクラス
    """
    message_dict = {}
    logger = None

    def __init__(self, message_dict, logger):
        self.message_dict = message_dict
        self.logger = logger

    def log_message(self, id):
        """
        log_message.jsonからログに出力するメッセージを取得する関数

        :param id: ログメッセージのID
        """
        try:
            message = self.message_dict[id]["message"]
            level = self.message_dict[id]["level"]
            ret_logger = None
            if level == "INFO":
                ret_logger = self.logger.info
            elif level == "ERROR":
                ret_logger = self.logger.error
            elif level == "WARNING":
                ret_logger = self.logger.warning
            return ret_logger, message
        except:
            self.logger.error("内部エラーが発生しました。")
            raise Exception


    def format_log_message(self, id, value_list):
        """
        log_message.jsonからログに出力する変数ありメッセージを取得する関数

        :param id: ログメッセージのID
        :param value_list: メッセージに適用する値のリスト
        """
        try:
            message = self.message_dict[id]["message"]

            formatted_message = message.format(*value_list)
            level = self.message_dict[id]["level"]
            ret_logger = None
            if level == "INFO":
                ret_logger = self.logger.info
            elif level == "ERROR":
                ret_logger = self.logger.error
            elif level == "WARNING":
                ret_logger = self.logger.warning
            return ret_logger, formatted_message
        except:
            self.logger.error("内部エラーが発生しました。")
            raise Exception
    
    def debug(self, message):
        """
        デバッグ用のメッセージを出力する関数
        :param message: デバッグ用のメッセージ
        """
        self.logger.debug(message)

def output_init(config_dir, logger):
    """
    LogWriterのインスタンスを作成して返却する関数

    :param config_dir: log_message.jsonが格納されているディレクトリのパス
    :return LogWriterクラスのインスタンス
    """
    message_file_path = config_dir + "log_message.json"
    if not os.path.exists(message_file_path):
        logger.error("メッセージファイルが存在しません。")
        raise Exception
    with open(message_file_path, "r", encoding = "utf_8") as fp:
        # log_message.jsonを読み込む
        message_dict = json.load(fp)
    # LogWriterクラスのオブジェクトを作成
    log_writer = LogWriter(message_dict, logger)
    return log_writer

