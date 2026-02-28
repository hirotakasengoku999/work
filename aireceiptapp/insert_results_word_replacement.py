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


def write_result(input_path, seikyunengetsu, target_code, is_target_word_only=False):
    # master
    import folder
    symaster_df = []
    simaster_df = []
    karutemaster_df = []
    symaster_list = []
    simaster_list = []
    karutemaster_list = []
    base = Path(folder.base())
    try:
        file = base / f'aireceipt/data/receipt_code/add_sy_{target_code}.csv'
        symaster_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object', index_col='コード')
        tmp_sy_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object')
        symaster_list = tmp_sy_df['コード'].to_list()
    except:
        pass
    try:
        file = base / f'aireceipt/data/receipt_code/add_si_{target_code}.csv'
        simaster_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object', index_col='コード')
        tmp_si_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object')
        simaster_list = tmp_si_df['コード'].to_list()
    except:
        pass
    try:
        file = base / f'aireceipt/data/receipt_code/add_karute_{target_code}.csv'
        karutemaster_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object')
        karutemaster_df['コード'] = 'karute_all_' + karutemaster_df['コード']
        karutemaster_list = karutemaster_df['コード'].to_list()
    except:
        pass

    rece_master_list = []
    for d in [symaster_df, simaster_df]:
        if len(d) > 0:
            rece_master_list.append(d)

    rece_master_df = []
    if len(rece_master_df) > 0:
        rece_master_df = pd.concat(rece_master_list)

    base_dict = {
        'カルテ番号等': "PATIENT_ID",
        '氏名': "PATIENT_NAMEJ",
        '_date': "MEDICAL_CARE_DATE",
        '入外': "IN_OUT",
        '診療科名': "DEPT",
        '算定項目名称': "CODENAME",
        '算定漏れ確率': "PREDICT_PROBA",
        '算定実績': "ZISSEKI",
        "算定漏れ根拠_1": "KENCHI01",
        "算定漏れ根拠_2": "KENCHI03",
        "算定漏れ根拠_3": "KENCHI05",
        "算定漏れ根拠_4": "KENCHI07",
        "算定漏れ根拠_5": "KENCHI09"
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
        if item_code == target_code:
            d = pd.read_csv(file, engine='python', encoding='cp932', dtype='object')
            list_file.append(d)
    # pandasのdataframeに変換
    if len(list_file) > 0:
        df_all = pd.concat(list_file)
        usecols = []
        for col in base_cols + karutemaster_list + symaster_list + simaster_list:
            if col in list(df_all.columns):
                usecols.append(col)
        df_all = df_all[usecols]
        # 値が全てゼロの列を除去
        for col in list(df_all.columns)[13:]:
            value_unique = list(df_all[col].unique())
            if col in karutemaster_list:
                value_unique = list(df_all[col].unique())
                if value_unique in [[0.0], ['0.0'], [-1.0], ['-1.0']]:
                    df_all = df_all.drop(col, axis=1)
            else:
                df_all = df_all.drop(col, axis=1)
        # 各列を処理して1.0を列名称に変換
        for col in list(df_all.columns)[13:]:
            df_all[col] = df_all[col].replace(1.0, col)
            df_all[col] = df_all[col].replace('1.0', col)
        # 各列の0.0を除去して左詰め
        outdf_datas = []
        for index, value in df_all.iterrows():
            row_list = list(value)
            target_words = row_list[13:]
            target_words2_tmp = [item for item in target_words if item != "0.0"]
            target_words2 = []
            # コードを名称に変換
            for tw2 in target_words2_tmp:
                try:
                    target_words2.append(rece_master_df.loc[int(tw2)]['名称'])
                except:
                    target_words2.append(tw2)
            if (not is_target_word_only) and (len(target_words2) < 5):
                default_cols = row_list[8:13]
                work_num = 0
                while len(target_words2) < 5:
                    target_words2.append(default_cols[work_num])
                    work_num += 1
            while len(target_words2) < 5:
                target_words2.append('')
            new_row_list = row_list[:8] + target_words2[:5]
            if len(new_row_list) > 13:
                print(target_words2)
            outdf_datas.append(new_row_list)
        columns = list(base_dict.keys())
        outdf = pd.DataFrame(outdf_datas, columns=columns)
        outdf['算定項目名称'] = code_name_dict[target_code] if target_code in code_name_dict.keys() else outdf['算定項目名称']
        outdf['CODE'] = target_code
        # データベースの列名にリネーム
        outdf = outdf.rename(columns=base_dict)
        # 足りない列を補完
        for i in ['02', '04', '06', '08', '10']:
            outdf[f"KENCHI{i}"] = 0.0
        for i in range(10, 51):
            outdf[f"KENCHI{str(i).zfill(2)}"] = ""
        # -1.0を''に置換
        for i in ['01', '03', '05', '07', '09']:
            outdf[f"KENCHI{i}"] = outdf[f"KENCHI{i}"].str.replace('-1.0', '')
            outdf[f"KENCHI{i}"] = outdf[f"KENCHI{i}"].where(outdf[f"KENCHI{i}"].notna(), "")
        list_file.append(outdf)
        import get_hanyo
        patient_id_length_ = get_hanyo.get_hanyo("SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'hos_info' AND CODE2 = 'patient_id_length'")
        patient_id_length = patient_id_length_[0]['CONTROLINT1'] if len(patient_id_length_) > 0 else 8
        outdf['PATIENT_ID'] = outdf['PATIENT_ID'].astype('str').str.zfill(patient_id_length)
        outdf['SEIKYUNENGETSU'] = seikyunengetsu
        outdf['USER_CHECK'] = '未'
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
    target_code = args[3]
    is_target_word_only = args[4]
    write_result(input_path, seikyunengetsu, target_code, is_target_word_only)

