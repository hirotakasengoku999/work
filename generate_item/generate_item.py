import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime

def get_config() -> list:
    file = "C:/aibrain_main/aireceiptproject/aireceipt/data/config/m8_model_creation.json"
    with open(file, encoding='utf-8') as f:
        config = json.load(f)
    targets = config["1"]["targets"]
    file = "C:/aibrain_main/aireceiptproject/aireceipt/data/config/r1_rule_execution.json"
    rule_list = []
    with open(file, encoding='utf-8') as f:
        config = json.load(f)
    for i, v in config.items():
        for ii, vv in v.items():
            if ii == "target":
                targets.append(vv)
                rule_list.append(vv)

    return targets, rule_list

def main(targets: list, rule_list: list):
    df = pd.read_csv("s.csv", engine='python', encoding='cp932', dtype='object', header=None)
    out_df = df[df[2].isin(targets)]
    out_df = out_df[[2, 4, 11]]
    # 2がユニークになるよう、同じコードが複数ある場合は最初を採用する
    out_df = out_df.drop_duplicates(subset=2, keep='first')
    # out_dfの列名をCODE, NAME, POINTSに変更する
    out_df.columns = ["CODE", "NAME", "POINTS"]
    out_df["CODE2"] = ""
    app_list = [
        ["150333210_1", "", "閉鎖循環式全身麻酔４（側臥位）", "6610"],
        ["150333210_2", "", "閉鎖循環式全身麻酔４（腹腔鏡下手術）", "6610"],
        ["113017610-113017710", "", "入院栄養食事指導料１", "260"],
        ["113001310-113002110", "", "悪性腫瘍特異物質治療管理料", "360"],
        ["190171910", "820100016", "救急医療管理加算１（一）吐血・喀血・脱水", "1050"],
        ["190171910", "820100017", "救急医療管理加算１（二）意識障害", "1050"],
        ["190171910", "820100018-1", "救急医療管理加算１（四）心不全", "1050"],
        ["190171910", "820100018-2", "救急医療管理加算１（三）呼吸不全", "1050"],
        ["190171910", "820100019", "救急医療管理加算１（五）急性薬物中毒", "1050"],
        ["190171910", "820100020", "救急医療管理加算１（六）ショック", "1050"],
        ["190171910", "820100023", "救急医療管理加算１（九）外傷", "1050"],
        ["190171910", "820100024", "救急医療管理加算１（十）緊急手術", "1050"]
    ]
    for a in app_list:
        if a[0] in targets or a[1] in targets:
            # CODE, CODE2, NAME, POINTSの順で追加する
            out_df = pd.concat([out_df, pd.DataFrame([[a[0], a[1], a[2], a[3]]], columns=["CODE", "CODE2", "NAME", "POINTS"])])
    out_df["USE_FLAG"] = "1"
    out_df["PREDICT_PROBA"] = 50
    out_df["RULE_FLAG"] = "0"
    for rule in rule_list:
        out_df.loc[out_df["CODE"] == rule, "RULE_FLAG"] = "1"
    out_df["STARTDATE"] = "2022-04-01"
    out_df["ENDDATE"] = "2099-12-31"
    out_df["NOTE"] = ""
    out_df["SPARE"] = ""
    out_df["CREATEUSER"] = "admin9"
    out_df["CREATEDATE"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_df["UPDATEUSER"] = "admin9"
    out_df["UPDATEDATE"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_df = out_df.sort_values(by=["CODE", "CODE2"])
    out_df.to_csv('out.csv', index=False, encoding='cp932')
    con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
    out_df.to_sql('aireceiptapp_kenchi', con=con, if_exists='append', index=None)


if __name__ == '__main__':
    targets, rule_list = get_config()
    main(targets, rule_list)