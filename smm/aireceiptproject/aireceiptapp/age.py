import datetime

def age(birthday):
    today = datetime.date.today()
    birthday = datetime.date(birthday.year, birthday.month, birthday.day)
    return (int(today.strftime("%Y%m%d")) - int(birthday.strftime("%Y%m%d"))) // 10000

if __name__ == '__main__':
    b = '19860905'
    birth = age(datetime.datetime.strptime(b, '%Y%m%d'))
    print(birth)