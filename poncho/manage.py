import argparse

from preprocess.dirty_populate import main as dirty_main
from preprocess.cleanup import main as cleanup_main

# Available actions
ACTIONS = [
    'createdirtydb',
    'cleanupdb',
]

parser = argparse.ArgumentParser(description='CLI to interact with poncho the chatbot')

parser.add_argument('action', choices=ACTIONS, help='Action to be performed')
parser.add_argument('-t', '--timeframe', nargs='+', help='Timeframes that have to processed')

args = parser.parse_args()

if args.action is 'createdirtydb':
    pass
elif args.action is 'cleanupdb':
    pass
