import os
import errno
from datetime import datetime
import time
import sqlite3

import pandas as pd
from sklearn.utils import shuffle

from poncho.utils.get_base_dir import get_base_dir


def create_table(cur):
    '''Create a table which will contain comment-reply pairs that do not have a negative score
    and do not contain null fields.

    Args:
        conn -> Database connection.
        cur -> Database cursor.

    Returns:
        Database cursor.
    '''
    sql = '''
    CREATE TABLE IF NOT EXISTS best_comment_reply
    AS
        SELECT 
            id, comment, reply
        FROM rc_cleaned
        WHERE
            comment_score >= 0 AND
            comment IS NOT NULL AND
            reply IS NOT NULL
    '''
    cur = cur.execute(sql)
    return cur


def main(timeframes):
    BASE_DIR = get_base_dir()

    # Try to create some required files if they do not exist already
    try:
        os.makedirs(os.path.join(BASE_DIR, 'data', 'prepared'))
        os.makedirs(os.path.join(BASE_DIR, 'data', 'logs'))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
        else:
            pass

    try:
        log = open(os.path.join(BASE_DIR, 'data', 'logs', 'prepared_{}'.format(str(time.time()).split('.')[0])), mode='a')
        
        log.write('Beginning preparation of data. Time: {}\n\n'.format(str(datetime.now())))
        print('Beginning preparation of data. Time: {}\n'.format(str(datetime.now())))

        for timeframe in timeframes:
            log.write('Preparing data of {}. Time: {}'.format(timeframe, str(datetime.now())))
            print('Preparing data of {}. Time: {}'.format(timeframe, str(datetime.now())))

            # Database connections
            clean_conn = sqlite3.connect(os.path.join(BASE_DIR, 'data', 'processed', 'RC_clean_{}.db'.format(timeframe.split('-')[0])))
            clean_cur = clean_conn.cursor()

            # Create the required table
            clean_cur = create_table(clean_cur)

            # Get all the data from the cleaned database to a Pandas DataFrame
            df = pd.read_sql_query('SELECT * FROM best_comment_reply', clean_conn, index_col='id')
            # Shuffle the data
            df = shuffle(df)
            # Write all the data to a CSV file
            df.to_csv(os.path.join(BASE_DIR, 'data', 'prepared', 'prepared_{}.csv'.format(timeframe.split('-')[0])), mode='a')

        log.write('Finishing up... Time: {}\n'.format(str(datetime.now())))
        log.write('===================================================================================\n\n')
        print('Finishing up... Time: {}'.format(str(datetime.now())))
    except Exception as e:
        raise e

    # Close connections
    clean_cur.close()
    clean_conn.close()
