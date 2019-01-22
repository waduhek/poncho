import sqlite3
import json
import time
from datetime import datetime

# Year and month of when the data was collected.
TIMEFRAMES = [
    '2018-01',
    '2018-02',
    '2018-03',
]

# Global lists to allow for more efficient insertion of data into the database.
TRANSACTIONS = []
TRANSACTION_ARGS = []


def create_tables(cur):
    '''Create two tables for the specified year; one to store the parent comments
    and the other to store replies to the parent.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    parent = '''
    CREATE TABLE IF NOT EXISTS rc_parent (
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


def reformat(text):
    '''Convert escape characters to strings and convert all double inverted commas to a single
    inverted comma.

    Args:
        text -> Text to be reformated.
    Returns:
        Reformatted text.
    '''
    return text.replace('\n', 'newlinechar').replace('\r', 'newlinechar').replace('"', "'")


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
        INSERT INTO rc_parent (comment_id, created_unix, score, comment, subreddit)
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
    if len(txt.split(' ')) > 50 or len(txt) < 1 or len(txt) > 10000:
        return False
    elif txt == '[removed]' or txt == '[deleted]':
        return False
    else:
        return True


if __name__ == '__main__':
    row_counter = 0

    for timeframe in TIMEFRAMES:
        # Database connection.
        conn = sqlite3.connect('../data/processed/RC_dirty_{}.db'.format(timeframe.split('-')[0]))
        cur = conn.cursor()

        cur = create_tables(cur)

        # Open the reddit comments file.
        with open('../data/raw/RC_{}'.format(timeframe), buffering=10000000) as data:
            # Log events.
            with open('../data/logs/dirty_{}.txt'.format(str(time.time()).split('.')[0]), mode='a') as log:
                print('Beginning to write comments to the database. Time: {}'.format(str(datetime.now())))
                log.write('Beginning to write comments to the database. Time: {}\n'.format(str(datetime.now())))
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
                            con, cur = insert_to_table(
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

                row_counter = 0
                print('Beginning to write replies to the database. Time: {}'.format(str(datetime.now())))
                log.write('Beginning to write replies to the database. Time: {}\n'.format(str(datetime.now())))
                # Insert all replies to the comments.
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
                            con, cur = insert_to_table(
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
