import faker
from datetime import datetime
import random
import psycopg2

fake=faker.Faker()


def gen_trans():
    user =fake.simple_profile()
    return {
    'trans_id':fake.uuid4(),
    'user_id':user['username'],
    'timestamp':datetime.utcnow().timestamp(),
    'amount':round(random.uniform(10,1000),2),
    'curr':random.choice(['INR','USD']),
    'city':fake.city(),
    'country':fake.country(),
    'merchantName':fake.company(),
    'paymentMethod':random.choice(['creditCard','debitCard','UPI']),
    'ipAdrr':fake.ipv4(),
    'voucherCode':random.choice(['','Discount10','']),
    'aff_id':fake.uuid4()
    }



def create_table(conn):
    curr=conn.cursor()
    curr.execute(
        '''
        CREATE TABLE IF NOT EXISTS transactions(
        trans_id varchar(255) PRIMARY KEY,
        user_id varchar(255),
        timestamp TIMESTAMP,
        amount decimal,
        curr varchar(255),
        city varchar(255),
        country varchar(255),
        merchantName varchar(255),
        paymentMethod varchar(255),
        ipAdrr varchar(255),
        voucherCode varchar(255),
        aff_id varchar(255)
        )
    '''
    )
    curr.close()
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect (
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
)


create_table(conn)
transaction = gen_trans()
curr = conn.cursor()
print(transaction)
curr.execute (
        """
        INSERT INTO transactions(trans_id, user_id, timestamp, amount, curr, city, country, merchantName, paymentMethod, 
        ipAdrr, aff_id, voucherCode)
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
              transaction["trans_id"],
              transaction["user_id"],
              datetime.fromtimestamp(transaction["timestamp"]).strftime ('%Y-%m-%d %H:%M:%S'),
              float(transaction["amount"]),
              transaction["curr"],
              transaction["city"],
              transaction["country"],
              transaction["merchantName"],
              transaction["paymentMethod"],
              transaction["ipAdrr"],
              transaction["aff_id"],
              transaction["voucherCode"]
    )
    )

curr.close ()
conn.commit ()
