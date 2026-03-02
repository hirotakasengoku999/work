import os, datetime
import folder

base = folder.base()

# 検知をタスクスケジューラに登録
cmd_file = "RegistTask.bat"
regist_task_dir = os.path.join(base,"regist_task")
date_today = datetime.date.today()
date_today_str = date_today.strftime('%Y-%m-%d')
date_time = datetime.datetime.now()
date_time_2min = date_time + datetime.timedelta(minutes=2)
date_time_2min_str = date_time_2min.strftime('%H:%M')
codes = ''
command = cmd_file + " " + "preprocessing_predict" + " " + date_today_str + " " + date_time_2min_str
os.chdir(regist_task_dir)
os.system(command)
os.chdir(base)

print(f'comannd = {command}')