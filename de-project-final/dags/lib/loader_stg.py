import pandas as pd


def load_transactions(pg_conn, date, path):
    load_path = f"{path}/transactions_{date}.csv"
    query = f"""
                    select *
                    from public.transactions
                    where transaction_dt::date = '{date}'
                """
    with pg_conn as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                transactions_df = pd.DataFrame(result)
                transactions_df.to_csv(path_or_buf=load_path,
                                       sep=';',
                                       header=False,
                                       index=False)
            except:
                raise Exception("An error occured while retrieving transactions data")
            
            
            
def load_currencies(pg_conn, date, path):
    load_path = f"{path}/currencies_{date}.csv"
    query = f"""
                    select *
                    from public.currencies
                    where date_update::date = '{date}'
                """
    with pg_conn as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                currencies_df = pd.DataFrame(result)
                currencies_df.to_csv(path_or_buf=load_path,
                                     sep=';',
                                     header=False,
                                     index=False)
            except:
                raise Exception("An error occured while retrieving currencies data")
            