import argparse
import re

from poncho.preprocess.dirty_populate import main as dirty_main
from poncho.preprocess.cleanup import main as cleanup_main
from poncho.preprocess.convert_to_csv import main as prepare_main

# Available actions
ACTIONS = [
    'createdirtydb',
    'cleanupdb',
    'preparedata',
]

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

# Parse the entered arguments
args = parser.parse_args()

if args.action == 'createdirtydb':
    if not args.timeframe:
        parser.error('-t is required when using "createdirtydb".')
    else:
        # Check the formatting of the timeframes provided
        pattern = r'^20[0-9]{2}\-(?:0[1-9]{1}|1[012]{1})$'
        regexp = re.compile(pattern)
        for timeframe in args.timeframe:
            if not regexp.match(timeframe):
                parser.error('{} is not a valid timeframe.'.format(timeframe))

        # Call the function to create the dirty database
        dirty_main(args.timeframe)

        # Check if "autoclean" option is set
        if args.autoclean:
            # Call the function to create the clean database
            cleanup_main(args.timeframe)

            # Check if "prepare" option is set
            if args.prepare:
                # Call function to prepare the data
                prepare_main(args.timeframe)
elif args.action == 'cleanupdb':
    if not args.timeframe:
        parser.error('-t is required when using "cleanupdb"')
    else:
        cleanup_main(args.timeframe)

        # Check if "prepare" option is set
        if args.prepare:
            # Call function to prepare data
            prepare_main(args.timeframe)
elif args.action == 'preparedata':
    if not args.timeframe:
        parser.error('-t is required when using "preparedata"')
    else:
        prepare_main(args.timeframe)
