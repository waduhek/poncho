import os
import time
from datetime import datetime

import pandas as pd

from poncho.utils.get_base_dir import get_base_dir


def main(unique_years):
    (BASE_DIR) = get_base_dir()

    # Log file
    log = open(os.path.join(BASE_DIR, 'data', 'logs', 'train_{}.txt'.format(str(time.time()).split('.')[0])), mode='a')
    
    log.write('Beginning to create training dataset. Time: {}\n\n'.format(str(datetime.now())))
    print('Beginning to create training dataset. Time: {}\n'.format(str(datetime.now())))

    for year in unique_years:
        log.write('Converting data of {} into train dataset\n. Time: {}'.format(year, str(datetime.now())))
        print('Converting data of {} into train dataset. Time: {}'.format(year, str(datetime.now())))

        # Open required CSV file
        df = pd.read_csv(os.path.join(BASE_DIR, 'data', 'prepared', 'prepared_{}.csv'.format(year))).dropna()

        # Convert the comments and replies to a Pandas DataFrame object
        comment = pd.DataFrame(df['comment'])
        reply = pd.DataFrame(df['reply'])

        # Write the comments and replies to separate files in the directory of 'nmt-chatbot'
        comment.to_csv(
            os.path.join(os.path.dirname(BASE_DIR), 'nmt-chatbot', 'new_data', 'train.from'),
            mode='w',
            index=False,
            header=None
        )
        reply.to_csv(
            os.path.join(os.path.dirname(BASE_DIR), 'nmt-chatbot', 'new_data', 'train.to'),
            mode='w',
            index=False,
            header=None
        )

    log.write('Finishing up... Time: {}\n'.format(str(datetime.now())))
    log.write('==========================================================================================\n\n')
    print('Finishing up... Time: {}'.format(str(datetime.now())))
    print('==========================================================================================')

    log.close()
