# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import MySQLdb
from sqlalchemy import create_engine
import datetime
import sys
from pathlib import Path

# DB情報
db_passwd = 'administrator'
db_name = 'aireceiptdb'


def conectdb(request_sql):
    engine = create_engine(f'mysql://root:{db_passwd}@127.0.0.1/{db_name}?charset=utf8')
    connection = engine.connect()
    result = connection.execute(request_sql)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    connection.close()
    return (df)


def write_result(input_path, seikyunengetsu, target_code, target_word):
    base_dict = {
        'カルテ番号等': 'PATIENT_ID',
        '氏名': 'PATIENT_NAMEJ',
        '_date': 'MEDICAL_CARE_DATE',
        '入外': 'IN_OUT',
        '診療科名': 'DEPT',
        '算定項目名称': 'CODENAME',
        '算定漏れ確率': 'PREDICT_PROBA',
        '算定実績': 'ZISSEKI',
        '算定漏れ根拠_1': 'KENCHI01',
        '算定漏れ根拠_2': 'KENCHI03',
        '算定漏れ根拠_3': 'KENCHI05',
        '算定漏れ根拠_4': 'KENCHI07',
        '算定漏れ根拠_5': 'KENCHI09'
    }
    base_cols = list(base_dict.keys())

    # result.csvファイルリストを作る
    files = Path(input_path).glob('**/*.csv')

    import get_kenchi
    code_name_dict = get_kenchi.get_code_name_dict()

    # csvファイルの結合
    list_file = []
    for file in files:
        # ファイル名からレセ電コードを取得
        item_code = file.name[:-4]
        # リハビリテーション総合計画評価料１ならリハビリテーション総合実施計画書という単語があれば、根拠１にもってくる
        if item_code == target_code:
            usecols = base_cols + [target_word]
            d = pd.read_csv(file, engine='python', usecols=usecols, encoding='cp932', dtype='object')
            list_file.append(d)
    # pandasのdataframeに変換
    if len(list_file) > 0:
        df_all = pd.concat(list_file)
        # target_word列の1.0を列名に変換
        df_all[target_word] = df_all[target_word].replace(1.0,target_word)
        df_all[target_word] = df_all[target_word].replace('1.0',target_word)
        # target_wordの記載があれば、根拠１にもってくる
        outdf_datas = []
        for index, value in df_all.iterrows():
            row_list = list(value)
            target_words = row_list[8:]
            if value[target_word] == target_word and target_word in target_words:
                target_words = [value[target_word]] + target_words
            if len(target_words) > 5:
                target_words = target_words[:5]
            new_row_list = row_list[:8] + target_words
            outdf_datas.append(new_row_list)
        columns = base_cols
        outdf = pd.DataFrame(outdf_datas, columns=columns)
        outdf['算定項目名称'] = code_name_dict[target_code]
        outdf['CODE'] = target_code
        # データベースの列名にリネーム
        outdf = outdf.rename(columns=base_dict)
        # 足りない列を補完
        for i in ['02', '04', '06', '08', '10']:
            outdf[f"KENCHI{i}"] = 0.0
        for i in range(10, 51):
            outdf[f"KENCHI{str(i).zfill(2)}"] = ""
        import get_hanyo
        patient_id_length_ = get_hanyo.get_hanyo("SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'hos_info' AND CODE2 = 'patient_id_length'")
        patient_id_length = patient_id_length_[0]['CONTROLINT1'] if len(patient_id_length_) > 0 else 8
        outdf['PATIENT_ID'] = outdf['PATIENT_ID'].astype('str').str.zfill(patient_id_length)
        outdf['SEIKYUNENGETSU'] = seikyunengetsu

        # 本レセ前か後か
        import ent
        try:
            outdf = ent.ent(seikyunengetsu, outdf, False)
        except:
            print('ent失敗')

        df_no_calculation_results = outdf[outdf['ZISSEKI'] == '0.0']
        result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM no_calculation_results"))
        outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
        df_no_calculation_results['OUTPUTDATE'] = outputdate
        df_calculation_exists_results = outdf[outdf['ZISSEKI'] == '1.0']
        result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM calculation_exists_results"))
        outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
        df_calculation_exists_results['OUTPUTDATE'] = outputdate
        con = create_engine(f'mysql://root:{db_passwd}@127.0.0.1/{db_name}?charset=utf8')
        df_no_calculation_results.to_sql('no_calculation_results', con=con, if_exists='append', index=None)
        df_calculation_exists_results.to_sql('calculation_exists_results', con=con, if_exists='append', index=None)

if __name__ == '__main__':
    args = sys.argv
    input_path = args[1]
    seikyunengetsu = args[2]
    output_datetime = ''
    target_code = args[3]
    target_word = args[4]

    write_result(input_path, seikyunengetsu, target_code, target_word)

