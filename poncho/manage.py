import argparse

from preprocess.dirty_populate import main as dirty_main
from preprocess.cleanup import main as cleanup_main

# Available actions
ACTIONS = [
    'createdirtydb',
    'cleanupdb',
]

# Initalise parser
parser = argparse.ArgumentParser(description='CLI to interact with poncho the chatbot')

# Add action argument
parser.add_argument('action', choices=ACTIONS, help='Action to be performed')
# Add timeframes option
parser.add_argument('-t', '--timeframe', nargs='+', help='Timeframes that have to processed')

# Parse the entered arguments
args = parser.parse_args()

if args.action == 'createdirtydb':
    if not args.timeframe:
        parser.error('-t is required when using "createdirtydb".')
    else:
        dirty_main(args.timeframe)
elif args.action == 'cleanupdb':
    if not args.timeframe:
        parser.error('-t is required when using "cleanupdb"')
    else:
        cleanup_main(args.timeframe)
