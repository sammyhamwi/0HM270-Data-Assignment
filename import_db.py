import pandas as pd
from sqlalchemy import create_engine

# MySQL connection (read source)
mysql_engine = create_engine('mysql+pymysql://root:root@localhost/rechtspraak')
df = pd.read_sql_table('rechtspraak', con=mysql_engine)

# SQLite connection (write target)
sqlite_engine = create_engine('sqlite:///rechtspraak_shared.db')
df.to_sql('rechtspraak', con=sqlite_engine, index=False, if_exists='replace')

print("Data exported to rechtspraak_shared.db (SQLite) successfully.")