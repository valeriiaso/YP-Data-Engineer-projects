import datetime


def get_previous_day(date):
    date_str = datetime.datetime.strptime(date, '%Y-%m-%d')
    previous_day = (date_str - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    return previous_day


def execute_query(vertica_connection, query):
    with vertica_connection as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(query)
            except:
                raise Exception("An error occurred while executing the query") 


def insert_h_transactions(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.h_transactions(h_transaction_pk, operation_id, load_dt, load_src)
                select distinct
                        hash(operation_id),
                        operation_id,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.transactions
                where transaction_dt::date = '{previous_day}'
                        and account_number_from >= 0
                        and hash(operation_id) not in (select h_transaction_pk from STV2023121116__DWH.h_transactions);
            """
    execute_query(vertica_connection, query)
            

def insert_h_currencies(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.h_currencies(h_currency_pk, currency_code, load_dt, load_src)
                select distinct 
                        hash(currency_code, date_update),
                        currency_code,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.currencies
                where date_update::date = '{previous_day}'
                        and hash(currency_code, date_update) not in (select h_currency_pk from STV2023121116__DWH.h_currencies);
            """
    execute_query(vertica_connection, query)


def insert_l_transaction_currency(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.l_transaction_currency(hk_transaction_currency_pk, h_transaction_pk, h_currency_pk, load_dt, load_src)
                select distinct
                        hash(h_transaction_pk, h_currency_pk),
                        ht.h_transaction_pk,
                        hc.h_currency_pk,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.transactions tr
                join STV2023121116__DWH.h_transactions ht
                on tr.operation_id = ht.operation_id
                join STV2023121116__DWH.h_currencies hc
                on tr.currency_code = hc.currency_code
                where tr.transaction_dt::date = '{previous_day}'
                        and hash(h_transaction_pk, h_currency_pk) not in (select hk_transaction_currency_pk from STV2023121116__DWH.l_transaction_currency);
            """
    execute_query(vertica_connection, query)


def insert_s_currencies_rates(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.s_currencies_rates(h_currency_pk, date_update, currency_code_with, currency_with_div, load_dt, load_src)
                select  hc.h_currency_pk,
                        cur.date_update,
                        cur.currency_code_with,
                        cur.currency_with_div,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.currencies cur
                join STV2023121116__DWH.h_currencies hc
                on cur.currency_code = hc.currency_code
                where cur.date_update::date = '{previous_day}'
            """
    execute_query(vertica_connection, query)


def insert_s_transactions_send_info(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.s_transactions_send_info(h_transaction_pk, account_number_from, account_number_to, country, transaction_type, load_dt, load_src)
                select distinct
                        h_transaction_pk,
                        account_number_from,
                        account_number_to,
                        country,
                        transaction_type,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.transactions tr
                join STV2023121116__DWH.h_transactions ht
                on tr.operation_id = ht.operation_id
                where tr.transaction_dt::date = '{previous_day}'
            """
    execute_query(vertica_connection, query)


def insert_s_transactions_amount(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.s_transactions_amount(h_transaction_pk, currency_code, amount, load_dt, load_src)
                select distinct
                        h_transaction_pk,
                        currency_code,
                        amount,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.transactions tr
                join STV2023121116__DWH.h_transactions ht
                on tr.operation_id = ht.operation_id
                where tr.transaction_dt::date = '{previous_day}'
            """
    execute_query(vertica_connection, query)


def insert_s_transactions_status(date, vertica_connection, source, load_dt):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.s_transactions_status(h_transaction_pk, status, transaction_dt, load_dt, load_src)
                select  h_transaction_pk,
                        status,
                        transaction_dt,
                        '{load_dt}'::timestamp as load_dt,
                        '{source}' as load_src
                from STV2023121116__STAGING.transactions tr
                join STV2023121116__DWH.h_transactions ht
                on tr.operation_id = ht.operation_id
                where tr.transaction_dt::date = '{previous_day}'
            """
    execute_query(vertica_connection, query)


def insert_global_metrics(date, vertica_connection):
    previous_day = get_previous_day(date)
    query = f"""
                insert into STV2023121116__DWH.global_metrics(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
                with tbl as(
                    select distinct
                            hc.currency_code,
                            ht.operation_id,
                            sts.transaction_dt,
                            st.account_number_from,
                            case
                                when hc.currency_code = 420 then sta.amount
                                when hc.currency_code != 420 then (sta.amount * scr.currency_with_div) 
                            end as amount_dollars,
                            row_number() over(partition by hc.currency_code, ht.operation_id, sts.transaction_dt::date, st.account_number_from) as row_num
                    from STV2023121116__DWH.h_transactions ht
                    join STV2023121116__DWH.s_transactions_status sts
                    on ht.h_transaction_pk = sts.h_transaction_pk
                    join STV2023121116__DWH.l_transaction_currency ltc
                    on ht.h_transaction_pk = ltc.h_transaction_pk
                    join STV2023121116__DWH.h_currencies hc
                    on hc.h_currency_pk = ltc.h_currency_pk
                    left join STV2023121116__DWH.s_currencies_rates scr
                    on hc.h_currency_pk = scr.h_currency_pk 
                    	and scr.date_update::date = '{previous_day}'
                        and scr.currency_code_with = 420
                    join STV2023121116__DWH.s_transactions_send_info st
                    on st.h_transaction_pk = ht.h_transaction_pk
                    join STV2023121116__DWH.s_transactions_amount sta
                    on sta.h_transaction_pk = ht.h_transaction_pk
                    where sts.transaction_dt::date = '{previous_day}'
                    		and transaction_type != 'authorization'
                )
                select  transaction_dt::date as date_update,
                        currency_code as currency_from,
                        sum(amount_dollars) as amount_total,
                        count(distinct operation_id) as cnt_transactions,
                        (count(distinct operation_id)/count(distinct account_number_from)) as avg_transactions_per_account,
                        count(distinct account_number_from) as cnt_accounts_make_transactions
                from tbl
                where row_num = 1
                group by transaction_dt::date,
                        currency_code;
            """
    execute_query(vertica_connection, query)