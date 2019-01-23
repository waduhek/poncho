import sqlite3

# Year and month of when the data was collected.
TIMEFRAMES = [
    '2018-01',
    '2018-02',
    '2018-03',
]


def create_clean_table(cur):
    '''Create the required table for inserting the cleaned up data.

    Args:
        cur -> Database cursor.
    Returns:
        Database cursor.
    '''
    sql = '''
    CREATE TABLE IF NOT EXISTS rc_cleaned (
        id INT PRIMARY KEY AUTOINCREMENT,
        comment TEXT,
        reply TEXT
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
    CREATE VIEW comment_reply
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


if __name__ == '__main__':
    paired_rows = 0

    try:
        for timeframe in TIMEFRAMES:
            # Database connections.
            dirty_conn = sqlite3.connect('../data/processed/RC_dirty_{}.db'.format(timeframe.split('-')[0]))
            dirty_cur = dirty_conn.cursor()
            clean_conn = sqlite3.connect('../data/processed/RC_clean_{}.db'.format(timeframe.split('-')[0]))
            clean_cur = clean_conn.cursor()

            # Create the table.
            clean_cur = create_clean_table(clean_cur)

            # Create a view containing comment, reply to the comment and the scores of the comment and reply.
            dirty_cur = create_view(dirty_cur)

            
    except Exception as e:
        raise e
    finally:
        # Close all database connections.
        dirty_cur.close()
        dirty_conn.close()
        clean_cur.close()
        clean_conn.close()
