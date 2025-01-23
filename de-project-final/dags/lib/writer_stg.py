import os


def write_data(vertica_conn, date, path, data_type):
    file_path = f"{path}/{data_type}_{date}.csv"
    query = f"""
        copy STV2023121116__STAGING.{data_type}(...)
        from local '{file_path}'
        delimiter ';'
        rejected data as table STV2023121116__STAGING.{data_type}_rejected
    """
    try:
        with vertica_conn.cursor() as cur:
            cur.execute(query)
    except Exception as e:
        raise Exception(f"An error occurred during writing {data_type} data to Vertica: {str(e)}")


def delete_files(path_transactions, path_currencies, date):
    path_transactions_full = f"{path_transactions}/transactions_{date}.csv"
    path_currencies_full = f"{path_currencies}/currencies_{date}.csv"
    os.remove(path_transactions_full)
    os.remove(path_currencies_full)