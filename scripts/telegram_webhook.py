import argparse
import json
import os

from telegram_helper import delete_bot_commands, delete_webhook, get_webhook_info, set_bot_commands, set_webhook


def main():
    parser = argparse.ArgumentParser(description='Manage Telegram webhook')
    subparsers = parser.add_subparsers(dest='command', required=True)
    set_parser = subparsers.add_parser('set')
    set_parser.add_argument('url')
    subparsers.add_parser('delete')
    subparsers.add_parser('info')
    subparsers.add_parser('commands-set')
    subparsers.add_parser('commands-delete')
    args = parser.parse_args()

    if args.command == 'set':
        secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET')
        if not secret:
            raise EnvironmentError('TELEGRAM_WEBHOOK_SECRET env is required')
        result = set_webhook(args.url, secret)
    elif args.command == 'delete':
        result = delete_webhook()
    elif args.command == 'info':
        result = get_webhook_info()
    elif args.command == 'commands-set':
        result = set_bot_commands()
    else:
        result = delete_bot_commands()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()