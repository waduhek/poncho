import argparse
import re

from poncho.preprocess.dirty_populate import main as dirty_main
from poncho.preprocess.cleanup import main as cleanup_main
from poncho.preprocess.convert_to_csv import main as prepare_main
from poncho.postprocess.prepare_data import main as train_data_main


# Available actions
ACTIONS = [
    'createdirtydb',
    'cleanupdb',
    'preparedata',
    'createtraindata',
]

def get_unique_years(timeframes):
    '''Get unique years from the 'timeframes' list.

    Args:
        timeframes -> Timeframes provided.
    Returns:
        List of all unique years.
    '''
    temp_timeframes = list()

    # Get the years from the list
    for timeframe in timeframes:
        temp_timeframes.append(timeframe.split('-')[0])

    # Remove duplicate years and return
    return list(set(temp_timeframes))


def regexp_check(timeframes):
    '''Perform a regular expression check for the provided timeframes.

    Args:
        timeframes -> Timeframes provided.
    Raises:
        ParserError if the timeframes provided are not correct.
    '''
    pattern = r'^20[0-9]{2}\-(?:0[1-9]{1}|1[012]{1})$'
    regexp = re.compile(pattern)
    for timeframe in args.timeframe:
        if not regexp.match(timeframe):
            parser.error('{} is not a valid timeframe.'.format(timeframe))

# Initalise parser
parser = argparse.ArgumentParser(description='CLI to interact with poncho the chatbot')

# Add action argument
parser.add_argument('action', choices=ACTIONS, help='Action to be performed.')
# Add timeframes option
parser.add_argument('-t', '--timeframe', nargs='+', help='Timeframes that have to processed.')
# Add autoclean option
parser.add_argument(
    '-a',
    '--autoclean',
    action='store_true',
    help='Autoexecute "cleanupdb" command after "createdirtydb" for the provided timeframes.'
)
# Add prepare option
parser.add_argument(
    '-p',
    '--prepare',
    action='store_true',
    help='Autoexecute "preparedata" command after "cleanupdb" for the provided timeframes.'
)
# Add createdata option
parser.add_argument(
    '-c',
    '--createdata',
    action='store_true',
    help='Autoexecute "createtraindata" command after "preparedata" for the provided timeframes.'
)

# Parse the entered arguments
args = parser.parse_args()

if args.action == 'createdirtydb':
    if not args.timeframe:
        parser.error('-t is required when using "createdirtydb".')
    else:
        # Check the formatting of the timeframes provided
        regexp_check(args.timeframes)

        # Call the function to create the dirty database
        dirty_main(args.timeframe)

        # Check if "autoclean" option is set
        if args.autoclean:
            # Call the function to create the clean database with the unique years
            cleanup_main(get_unique_years(args.timeframe))

            # Check if "prepare" option is set
            if args.prepare:
                # Call function to prepare the data with the unique years
                prepare_main(get_unique_years(args.timeframe))

                # Check if "createdata" option is set
                if args.createdata:
                    # Call the function to create the training dataset with unique years
                    train_data_main(get_unique_years(args.timeframe))
elif args.action == 'cleanupdb':
    if not args.timeframe:
        parser.error('-t is required when using "cleanupdb"')
    else:
        # Check the formatting of the timeframes provided
        regexp_check(args.timeframe)

        # Call the required function with the unique years
        cleanup_main(get_unique_years(args.timeframe))

        # Check if "prepare" option is set
        if args.prepare:
            # Call function to prepare data with the unique years
            prepare_main(get_unique_years(args.timeframe))

            # Check if "createdata" option is set
            if args.createdata:
                # Call the function to create the training dataset with unique years
                train_data_main(get_unique_years(args.timeframe))
elif args.action == 'preparedata':
    if not args.timeframe:
        parser.error('-t is required when using "preparedata"')
    else:
        # Check the formatting of the timeframes provided
        regexp_check(args.timeframe)

        # Call the function with the unique years
        prepare_main(get_unique_years(args.timeframe))

        # Check if "createdata" option is set
        if args.createdata:
            # Call the function to create the training dataset with unique years
            train_data_main(get_unique_years(args.timeframe))
elif args.action == 'createtraindata':
    if not args.timeframe:
        parser.error('-t is required when using "createtraindata"')
    else:
        # Check formatting of the timeframes provided
        regexp_check(args.timeframe)

        # Call the function with the unique years
        train_data_main(get_unique_years(args.timeframe))
