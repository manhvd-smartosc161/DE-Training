import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database connection configuration
DB_CONFIG = {
    'dbname': 'parch-and-posey',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

START_ID = 7020

# Create connection string
DB_URI = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Create engine
engine = create_engine(DB_URI)
Session = sessionmaker(bind=engine)

def random_date(start, end):
    """Generate random date between start and end"""
    delta = end - start
    int_delta = delta.days * 24 * 60 * 60 + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def generate_order(order_id):
    account_id = random.randint(1000, 2000)
    occurred_at = random_date(datetime(2015, 1, 1), datetime(2017, 1, 1))
    standard_qty = random.randint(0, 600)
    gloss_qty = random.randint(0, 600)
    poster_qty = random.randint(0, 600)
    total = standard_qty + gloss_qty + poster_qty
    standard_amt_usd = round(standard_qty * random.uniform(4, 6), 2)
    gloss_amt_usd = round(gloss_qty * random.uniform(6, 8), 2)
    poster_amt_usd = round(poster_qty * random.uniform(8, 10), 2)
    total_amt_usd = round(standard_amt_usd + gloss_amt_usd + poster_amt_usd, 2)
    print(f"Generated order {order_id}: account_id={account_id}, occurred_at={occurred_at}")
    return {
        'id': order_id,
        'account_id': account_id,
        'occurred_at': occurred_at,
        'standard_qty': standard_qty,
        'gloss_qty': gloss_qty,
        'poster_qty': poster_qty,
        'total': total,
        'standard_amt_usd': standard_amt_usd,
        'gloss_amt_usd': gloss_amt_usd,
        'poster_amt_usd': poster_amt_usd,
        'total_amt_usd': total_amt_usd
    }

def insert_orders(n=10):
    session = Session()
    try:
        print(f"Starting to insert {n} orders...")
        for i in range(n):
            order = generate_order(START_ID + i)
            stmt = text("""
                INSERT INTO orders (id, account_id, occurred_at, standard_qty, gloss_qty, poster_qty, total, standard_amt_usd, gloss_amt_usd, poster_amt_usd, total_amt_usd)
                VALUES (:id, :account_id, :occurred_at, :standard_qty, :gloss_qty, :poster_qty, :total, :standard_amt_usd, :gloss_amt_usd, :poster_amt_usd, :total_amt_usd)
            """)
            session.execute(stmt, order)
            print(f"Inserted order {order['id']}")
        session.commit()
        print(f"Successfully committed {n} orders to database")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()
        print("Session closed")

if __name__ == '__main__':
    print("Script started")
    insert_orders(200000)  # Add 200000 new rows
    print("Script completed")
