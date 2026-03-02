import subprocess, sys, traceback, os, time, datetime
import MySQLdb


def test_regist_task():
    regist_task_dir = 'C:/aibrain_main/aireceiptproject/regist_task/run_insert_sql.bat'
    args = [regist_task_dir]
    proc_model = subprocess.Popen(args)
    while True:
        time.sleep(10)
        if proc_model.poll() is not None:
            break
    rcode = proc_model.returncode


def regist_task():
    # 検知結果バックアップをタスクスケジューラに登録
    import folder
    base = folder.base()
    app_dir = base + 'aireceiptapp'
    cmd_file = 'RegistTask.bat'
    regist_task_dir = base + 'regist_task/'
    proc_name = 'run_insert_sql'
    result = ''
    run_datetime_d = datetime.datetime.now() + datetime.timedelta(minutes=30)
    run_date_s = run_datetime_d.strftime('%Y-%m-%d')
    run_time_s = run_datetime_d.strftime('%H:%M')
    os.chdir(regist_task_dir)
    args = [cmd_file, proc_name, run_date_s, run_time_s]
    proc_insert_backup = subprocess.Popen(args)
    while True:
        time.sleep(0.5)
        print(proc_insert_backup.poll())
        if proc_insert_backup.poll() is not None:
            break
    rcode = proc_insert_backup.returncode

    if rcode == 0:
        result = '検知結果バックアップのタスク登録が正常に行われました'
    else:
        result = rcode

    os.chdir(app_dir)

    return (result)


if __name__ == '__main__':
    regist_task()