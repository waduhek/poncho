from __future__ import absolute_import

import sqlite3
import json
import time
from datetime import datetime
import os
import errno

from poncho.utils.get_base_dir import get_base_dir
from poncho.utils.transaction_builder import transaction_builder
from poncho.utils.reformat import reformat


def create_tables(cur):
    '''Create two tables for the specified year; one to store the parent comments
    and the other to store replies to the parent.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    parent = '''
    CREATE TABLE IF NOT EXISTS rc_comment (
        comment_id TEXT PRIMARY KEY,
        created_unix INT,
        score INT,
        comment TEXT,
        subreddit TEXT
    );
    '''
    reply = '''
    CREATE TABLE IF NOT EXISTS rc_reply (
        comment_id TEXT PRIMARY KEY,
        parent_id TEXT,
        created_unix INT,
        score INT,
        comment TEXT,
        subreddit TEXT,
        FOREIGN KEY(parent_id) REFERENCES rc_parent(comment_id)
    )
    '''
    cur.execute(parent)
    cur.execute(reply)

    return cur


def insert_to_table(conn, cur, has_parent=False, **kwargs):
    '''Generate an INSERT statement to insert a Reddit comment into the database. The statement will
    then be passed to another function to create a transaction.

    Args:
        conn -> Database connection.
        cur -> Database cursor.
        has_parent -> Default: False. Set to True if the comment is a reply to another comment.
        comment_id -> ID of the comment. Primary Key in database.
        parent_id -> Required only if the comment is a reply (has_parent=True).
        score -> Score of the comment.
        subreddit -> Subreddit of the comment.
        created_unix -> Unix time of the comment.
        comment -> Body of the comment.
    Raises:
        KeyError if any of the required arguments are not present.
    Returns:
        Database connection.
        Database cursor.
    '''
    if 'comment_id' not in kwargs:
        raise KeyError('Missing Argument: comment_id')
    elif 'score' not in kwargs:
        raise KeyError('Missing Argument: score')
    elif 'created_unix' not in kwargs:
        raise KeyError('Missing Argument: created_unix')
    elif 'comment' not in kwargs:
        raise KeyError('Missing Argument: comment')
    elif 'subreddit' not in kwargs:
        raise KeyError('Missing Argument: subreddit')

    if has_parent:
        if 'parent_id' not in kwargs:
            raise KeyError('Missing Argument: parent_id')
        else:
            ins_parent = '''
            INSERT INTO rc_reply (comment_id, parent_id, created_unix, score, comment, subreddit)
            VALUES (?, ?, ?, ?, ?, ?)
            '''
            args = [
                kwargs['comment_id'],
                kwargs['parent_id'],
                kwargs['created_unix'],
                kwargs['score'],
                kwargs['comment'],
                kwargs['subreddit'],
            ]

            conn, cur = transaction_builder(conn, cur, ins_parent, args)
    else:
        ins_no_parent = '''
        INSERT INTO rc_comment (comment_id, created_unix, score, comment, subreddit)
        VALUES (?, ?, ?, ?, ?)
        '''
        args = [
            kwargs['comment_id'],
            kwargs['created_unix'],
            kwargs['score'],
            kwargs['comment'],
            kwargs['subreddit'],
        ]

        conn, cur = transaction_builder(conn, cur, ins_no_parent, args)

    return conn, cur


def acceptable(txt):
    '''Decide whether a string is acceptable or not. This function will check the length of the
    string and if the comment has been deleted or removed.

    Args:
        txt -> String to be checked.
    Returns:
        True if the string is acceptable.
        False if the string is not acceptable.
    '''
    if len(txt.split(' ')) > 50 or len(txt) < 1 or len(txt) > 1000:
        return False
    elif txt == '[removed]' or txt == '[deleted]':
        return False
    else:
        return True


def main(timeframes):
    row_counter = 0

    # Base Directory
    BASE_DIR = get_base_dir()

    # Check if some directories exist or not
    try:
        os.makedirs(os.path.join(BASE_DIR, 'data', 'logs'))
        os.makedirs(os.path.join(BASE_DIR, 'data', 'raw'))
        os.makedirs(os.path.join(BASE_DIR, 'data', 'processed'))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
        else:
            pass

    try:
        # Logs
        log = open(
            os.path.join(BASE_DIR, 'data', 'logs', 'dirty_{}.txt'.format(str(time.time()).split('.')[0])),
            mode='a'
        )

        for timeframe in timeframes:
            # Database connection.
            conn = sqlite3.connect(os.path.join(BASE_DIR, 'data', 'processed', 'RC_dirty_{}.db'.format(timeframe.split('-')[0])))
            cur = conn.cursor()

            cur = create_tables(cur)

            # Open the reddit comments file.
            with open(os.path.join(BASE_DIR, 'data', 'raw', 'RC_{}'.format(timeframe)), buffering=10000000) as data:
                print('Beginning to write comments of {} to the database. Time: {}\n'.format(timeframe, str(datetime.now())))
                log.write('Beginning to write comments of {} to the database. Time: {}\n\n'.format(timeframe, str(datetime.now())))
                # Insert all comments to the database.
                for row in data:
                    row_counter += 1

                    # Load the data row as JSON.
                    row = json.loads(row)

                    # Required data.
                    parent_id = row['parent_id']
                    comment = reformat(row['body'])
                    created_unix = row['created_utc']
                    score = row['score']
                    comment_id = row['id']
                    subreddit = row['subreddit']

                    if parent_id.split('_')[0] == 't3':
                        if acceptable(comment):
                            conn, cur = insert_to_table(
                                conn,
                                cur,
                                comment=comment,
                                created_unix=created_unix,
                                comment_id=comment_id,
                                subreddit=subreddit,
                                score=score
                            )

                    if row_counter % 10000 == 0:
                        print('No. of rows processed: {}. Time: {}'.format(row_counter, str(datetime.now())))
                        log.write('No. of rows processed: {}. Time: {}\n'.format(row_counter, str(datetime.now())))

            log.write('\nDone writing comments of {} to the database\n\n'.format(timeframe))

            # Insert replies for the comments.
            with open(os.path.join(BASE_DIR, 'data', 'raw', 'RC_{}'.format(timeframe)), buffering=10000000) as data:
                row_counter = 0

                print('Beginning to write replies of {} to the database. Time: {}'.format(timeframe, str(datetime.now())))
                log.write('\nBeginning to write replies of {} to the database. Time: {}\n\n'.format(timeframe, str(datetime.now())))

                for row in data:
                    row_counter += 1

                    # Load the data row as JSON.
                    row = json.loads(row)

                    # Required data.
                    parent_id = row['parent_id']
                    comment = reformat(row['body'])
                    created_unix = row['created_utc']
                    score = row['score']
                    comment_id = row['id']
                    subreddit = row['subreddit']

                    if parent_id.split('_')[0] == 't1':
                        if acceptable(comment):
                            conn, cur = insert_to_table(
                                conn,
                                cur,
                                has_parent=True,
                                parent_id=parent_id.split('_')[1],
                                comment=comment,
                                created_unix=created_unix,
                                comment_id=comment_id,
                                subreddit=subreddit,
                                score=score
                            )

                    if row_counter % 10000 == 0:
                        print('No. of rows processed: {}. Time: {}'.format(row_counter, str(datetime.now())))
                        log.write('No. of rows processed: {}. Time: {}\n'.format(row_counter, str(datetime.now())))
            
            log.write('\nDone writing replies of {} to the database\n\n'.format(timeframe))

            # Print and log finishing statements.
            print('Finishing entering data to the database. Time: {}'.format(str(datetime.now())))
            log.write('\nFinishing entering data to the database. Time: {}\n'.format(str(datetime.now())))
            log.write('===========================================================================\n\n')
            log.close()
    except Exception as e:
        raise e

    cur.close()
    conn.close()
