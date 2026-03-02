import subprocess, time

def sub():
  args = ["python", 'write_inspection.py']
  proc_receipt_pp = subprocess.Popen(args)
  while True:
    time.sleep(3)
    if proc_receipt_pp.poll() is not None:
      break
  rcode = proc_receipt_pp.returncode
  print(rcode)

if __name__ == '__main__':
  # 引数を１つずつ表示
  sub()