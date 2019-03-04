from __future__ import absolute_import

import sqlite3
from datetime import datetime
import time
import os
import errno
from urllib.request import pathname2url

from poncho.utils.transaction_builder import transaction_builder
from poncho.utils.get_base_dir import get_base_dir


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
    cur = cur.execute(sql)

    return cur


def create_view(cur):
    '''Create a view containing the comment, reply to the comment and the score of the comment and reply.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    sql = '''
    CREATE VIEW IF NOT EXISTS comment_reply
    AS
        SELECT
            rc_comment.comment AS comment,
            rc_reply.comment AS reply,
            rc_comment.score AS comment_score,
            rc_reply.score AS reply_score
        FROM rc_comment INNER JOIN rc_reply ON rc_comment.comment_id = rc_reply.parent_id
    '''
    try:
        cur = cur.execute(sql)
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
    cur = cur.execute(query)

    return cur


def insert_comment_and_reply(conn, cur, **kwargs):
    '''Insert the obtained comment and reply to the new table.

    Args:
        conn -> Database connection.
        cur -> Database cursor.
        Required keyword arguments: comment, reply, comment_score, reply_score.
    Returns:
        Database connection.
        Database cursor.
    '''
    if 'comment' not in kwargs:
        raise KeyError('Missing Argument: comment')
    elif 'reply' not in kwargs:
        raise KeyError('Missing Argument: reply')
    elif 'comment_score' not in kwargs:
        raise KeyError('Missing Argument: comment_score')
    elif 'reply_score' not in kwargs:
        raise KeyError('Missing Argument: reply_score')

    query = '''
    INSERT INTO rc_cleaned (comment, reply, comment_score, reply_score)
    VALUES (?, ?, ?, ?)
    '''

    args = [
        kwargs['comment'],
        kwargs['reply'],
        kwargs['comment_score'],
        kwargs['reply_score'],
    ]

    conn, cur = transaction_builder(conn, cur, query, args)

    return conn, cur


def main(unique_years):
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

        for year in unique_years:
            log.write('Cleaning data of {}. Time: {}\n\n'.format(year, str(datetime.now())))

            # Database connections.
            try:
                dirty_conn = sqlite3.connect(
                    'file:{}?mode=ro'.format(
                        pathname2url(
                            os.path.join(BASE_DIR, 'data', 'processed', 'RC_dirty_{}.db'.format(year))
                        )
                    ),
                    uri=True
                )
                dirty_cur = dirty_conn.cursor()
            except sqlite3.OperationalError:
                print('Error: RC_dirty_{0}.db does not exist.\nYou may have forgotten to run "createdirtydb" or have deleted the required database file.'.format(year))
                exit(errno.EIO)

            clean_conn = sqlite3.connect(
                'file:{}?mode=rwc'.format(
                    pathname2url(
                        os.path.join(BASE_DIR, 'data', 'processed', 'RC_clean_{}.db'.format(year))
                    )
                ),
                uri=True
            )
            clean_cur = clean_conn.cursor()

            # Create the table.
            clean_cur = create_clean_table(clean_cur)

            # Create a view containing comment, reply to the comment and the scores of the comment and reply.
            dirty_cur = create_view(dirty_cur)

            # Get the best comment and replies.
            dirty_cur = get_best_comment_and_replies(dirty_cur)

            # Insert the values into the database.
            while True:
                row = dirty_cur.fetchone()

                # Check if 'row' is empty.
                if not row:
                    break

                row_counter += 1

                # Pass the result to be inserted into the database.
                clean_conn, clean_cur = insert_comment_and_reply(
                    clean_conn,
                    clean_cur,
                    comment=row[0],
                    reply=row[1],
                    comment_score=row[2],
                    reply_score=row[3]
                )

                if row_counter % 10000 == 0:
                    print('No. of rows processed: {}. Time: {}'.format(row_counter, str(datetime.now())))
                    log.write('No. of rows processed: {}. Time: {}\n'.format(row_counter, str(datetime.now())))

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
