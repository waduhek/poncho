from __future__ import absolute_import

import sqlite3
from datetime import datetime
import time
import os
import errno

from utils.transaction_builder import transaction_builder
from utils.get_base_dir import get_base_dir


# Global lists to allow for more efficient insertion of data into the database.
TRANSACTIONS = []
TRANSACTION_ARGS = []


def create_clean_table(cur):
    '''Create the required table for inserting the cleaned up data.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    sql = '''
    CREATE TABLE IF NOT EXISTS rc_cleaned (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment TEXT,
        reply TEXT,
        comment_score INT,
        reply_score INT
    )
    '''
    cur.execute(sql)

    return cur


def create_view(cur):
    '''Create a view containing the comment, reply to the comment and the score of the comment and reply.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    sql = '''
    CREATE TEMPORARY VIEW comment_reply
    AS
        SELECT
            rc_comment.comment AS comment,
            rc_reply.comment AS reply,
            rc_comment.score AS comment_score,
            rc_reply.score AS reply_score
        FROM rc_comment INNER JOIN rc_reply ON rc_comment.comment_id = rc_reply.parent_id
    '''
    try:
        cur.execute(sql)
    except Exception as e:
        raise e

    return cur


def get_best_comment_and_replies(cur):
    '''Query the database to obtain a reply to the comment with the best possible score.
    The result of the query will NOT be fetched by this function.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    query = '''
    SELECT comment, reply, comment_score, MAX(reply_score) AS max_reply_score
    FROM comment_reply
    GROUP BY comment
    '''
    cur.execute(query)

    return cur


def insert_comment_and_reply(conn, cur, val):
    '''Insert the obtained comment and reply to the new table.

    Args:
        conn -> Database connection.
        cur -> Database cursor.
        val -> Data to be entered. Has to be an iterable (list or tuple). Note that 'val' must contain values for just one
               row i.e. use cursor.fetchone() only.
    Raises:
        TypeError if 'val' is not an iterable (list or tuple).
    Returns:
        Database connection.
        Database cursor.
    '''
    # Check type of the arguments passed.
    if type(val) is not list or type(val) is not tuple:
        raise TypeError('Argument: val is not an iterable.')

    query = '''
    INSERT INTO rc_cleaned (comment, reply, comment_score, reply_score)
    VALUES (?, ?, ?, ?)
    '''

    conn, cur = transaction_builder(conn, cur, query, val)

    return conn, cur


def main(timeframes):
    row_counter = 0

    # Base Directory
    BASE_DIR = get_base_dir()

    # Check if some directories exist or not
    try:
        os.makedirs(os.path.join(BASE_DIR, 'data', 'logs'))
        os.makedirs(os.path.join(BASE_DIR, 'data', 'processed'))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
        else:
            pass

    try:
        log = open(os.path.join(BASE_DIR, 'data', 'logs', 'clean_{}.txt'.format(str(time.time()).split('.')[0])), mode='a')

        log.write('Beginning to cleanup the data from the database. Time: {}\n\n'.format(str(datetime.now())))
        print('Beginning to cleanup the data from the database. Time: {}\n'.format(str(datetime.now())))

        for timeframe in timeframes:
            # Database connections.
            dirty_conn = sqlite3.connect(os.path.join(BASE_DIR, 'data', 'processed', 'RC_dirty_{}.db'.format(timeframe.split('-')[0])))
            dirty_cur = dirty_conn.cursor()

            clean_conn = sqlite3.connect(os.path.join(BASE_DIR, 'data', 'processed', 'RC_clean_{}.db'.format(timeframe.split('-')[0])))
            clean_cur = clean_conn.cursor()

            # Create the table.
            clean_cur = create_clean_table(clean_cur)

            # Create a view containing comment, reply to the comment and the scores of the comment and reply.
            dirty_cur = create_view(dirty_cur)

            # Get the best comment and replies.
            dirty_cur = get_best_comment_and_replies(dirty_cur)

            # Insert the values into the database.
            while True:
                rows = dirty_cur.fetchmany(1000)

                # Check if 'rows' is empty.
                if not rows:
                    break

                # Pass the result to be inserted into the database.
                for row in rows:
                    row_counter += 1

                    dirty_conn, dirty_cur = insert_comment_and_reply(dirty_conn, dirty_cur, list(row))

                if row_counter % 10000 == 0:
                    print('No. of rows processed: {}. Time: {}'.format(len(rows), str(datetime.now())))
                    log.write('No. of rows processed: {}. Time: {}\n'.format(len(rows), str(datetime.now())))

        # Print and log the finishing statement.
        print('Finishing up.. Time: {}'.format(str(datetime.now())))
        log.write('\nFinishing up.. Time: {}\n'.format(str(datetime.now())))
        log.write('==================================================================================\n\n')
        log.close()
    except Exception as e:
        raise e

    # Close all database connections.
    dirty_cur.close()
    dirty_conn.close()

    clean_cur.close()
    clean_conn.close()
