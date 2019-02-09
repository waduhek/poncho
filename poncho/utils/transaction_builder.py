# Global lists to allow for more efficient insertion of data into the database.
TRANSACTIONS = []
TRANSACTION_ARGS = []


def transaction_builder(conn, cur, sql, args):
    '''Build a transaction and execute SQL statements after 1000 SQL statements have been accumulated.
    Requires 2 lists: TRANSACTIONS and TRANSACTION_ARGS to be defined.

    Args:
        conn -> Database connection.
        cur -> Database cursor.
        sql -> SQL statement to be executed.
        args -> Values to be inserted for the corresponding query.
    Returns:
        Database connection.
        Database cursor.
    '''
    global TRANSACTIONS
    global TRANSACTION_ARGS

    TRANSACTIONS.append(sql)
    TRANSACTION_ARGS.append(args)

    # Execute transaction once the number of queries reaches 1000
    if len(TRANSACTIONS) > 1000:
        cur.execute('BEGIN TRANSACTION')

        for trans, args in zip(TRANSACTIONS, TRANSACTION_ARGS):
            try:
                cur.execute(trans, args)
            except Exception as e:
                pass

        conn.commit()
        TRANSACTIONS, TRANSACTION_ARGS = [], []

    return conn, cur
