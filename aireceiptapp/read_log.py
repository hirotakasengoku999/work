import glob, os, datetime, MySQLdb, re


# 引数に指定した日付に対して有効なヘッダーレコードを取得する
def get_aireceiptdb(request_sql):
    # データベースへの()接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    sql = (request_sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    connection.close()
    return (rows)


def read_log(proc, log_dir):
    # ログファイルの情報をを取得する
    files = os.listdir(log_dir)
    logfiles = [f for f in files if 'aireceipt' in f]

    d = {}
    for file in logfiles:
        target_file = os.path.join(log_dir, file)
        t = os.stat(target_file).st_mtime
        d[t] = file

    nd = sorted(d.items(), reverse=True)
    progress_proba_dict = {'1': 30, '2': 40, '3': 50, '4': 60, '5': 70, '6': 80, '7': 90, '8': 98}
    program_first_str = 'm'
    filename = 'create_model'
    if proc == 'predict':
        progress_proba_dict = {'1': 27, '2': 34, '3': 41, '4': 48, '5': 55, '6': 62, '7': 70, '8': 80, '9': 90}
        program_first_str = 'p'
        filename = 'predict_receipt'


    progress_proba = 20
    break_flag = False
    for n in nd:
        if not break_flag:
            target_file = os.path.join(log_dir, n[1])
            with open(target_file, encoding='cp932') as f:
                l = f.readlines()

            for i in reversed(l):

                work_list = i.split()
                if len(work_list) > 4 and '復帰値は次の通りです' in work_list[4] and filename in work_list[3]:
                    # モデル作成（または検知）の開始時間を取得する
                    process_name = ''
                    if 'predict' in proc:
                        process_name = '検知データ作成'
                    elif 'model' in proc:
                        process_name = 'モデル作成'
                    request_sql = "SELECT STARTDATE FROM aireceiptapp_userlog WHERE CODE = '" + process_name + "' ORDER BY STARTDATE DESC LIMIT 1"
                    rows = get_aireceiptdb(request_sql)
                    proc_start_time = None
                    for row in rows:
                        proc_start_time = row['STARTDATE']
                    # 直近の完了日時
                    log_enddate_s =work_list[0] + ' ' + work_list[1]
                    log_enddate = datetime.datetime.strptime(log_enddate_s, '%Y/%m/%d %H:%M:%S')
                    if log_enddate > proc_start_time:
                        if 'predict' in proc:
                            progress_proba = 90
                        else:
                            progress_proba = 100
                    break_flag = True
                    break

                elif len(work_list) > 4 and work_list[3][:1] == program_first_str and re.fullmatch('[0-9]+', work_list[3][1:2]) and 'を完了しました。' in work_list[4]:
                    print(i)
                    proba_num = work_list[3][1:2]
                    progress_proba = progress_proba_dict[proba_num]
                    break_flag = True
                    break
    return(progress_proba)
    # if len(work_list) >= 3:
    #     if work_list[2] == '[ERROR]':
    #         print(work_list)

# # 単体テスト　詳細取り出し
# if __name__ == '__main__':
#     progress_proba = check_pp()
#     if progress_proba != 100 and progress_proba >= 20:
#         log_dir = 'C:/Users/user/Desktop/read_log/'
#         progress_proba = read_log('predict', log_dir)
#     log_dir = 'C:/Users/user/Desktop/read_log/'
#     progress_proba = read_log('predict', log_dir)
