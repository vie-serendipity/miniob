import random
import datetime
import string

keys = {}
tuples = []

def random_date(start, end):
    return (start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )).strftime('%Y-%m-%d')

def random_string(cap):
    length = random.randint(1, cap)
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def gen_big_write():
    ft = open('test/primary-big-write.test', 'w', encoding='utf-8')
    fr = open('result/primary-big-write.result', 'w', encoding='utf-8')

    ft.write('CREATE TABLE big_write_0(id int, num int, price float, addr char(100), birthday date);\n\n')
    fr.write('CREATE TABLE big_write_0(id int, num int, price float, addr char(100), birthday date);\nSUCCESS\n\n')

    for i in range(2276):
        tuple = {
            'id': random.randint(0, 10000),
            'num': random.randint(0, 10000),
            'price': round(random.uniform(0.0, 1000.0), 2),
            'addr': random_string(100),
            'birthday': random_date(datetime.date(1970, 1, 1), datetime.date(2038, 12, 31)),
        }
        while tuple['id'] in keys.keys():
            tuple['id'] = random.randint(0, 10000)
        keys[tuple['id']] = 1
        tuples.append(tuple)
        ft.write(f"INSERT INTO big_write_0 VALUES ({tuple['id']}, {tuple['num']}, {tuple['price']}, '{tuple['addr']}', '{tuple['birthday']}');\n")
        fr.write(f"INSERT INTO big_write_0 VALUES ({tuple['id']}, {tuple['num']}, {tuple['price']}, '{tuple['addr']}', '{tuple['birthday']}');\nSUCCESS\n")
        if tuple['price'].is_integer():
            tuple['price'] = int(tuple['price'])

    for i in range(600):
        idx = random.randint(0, len(tuples))
        tuples[idx]['num'] = 

    ft.write('\nselect * from big_write_0;\n')
    fr.write('\nselect * from big_write_0;\n')
    fr.write('ID | NUM | PRICE | ADDR | BIRTHDAY\n')
    for tuple in tuples:
        fr.write(f"{tuple['id']} | {tuple['num']} | {tuple['price']} | {tuple['addr']} | {tuple['birthday']}\n")

if __name__ == '__main__':
    gen_big_write()
