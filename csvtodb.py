from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = ('Creates a new table based on the input csv file. Used in '
            'conjunction with inspectdb command to generate django models')

    def add_arguments(self, parser):
        parser.add_argument('table_name', nargs=1, type=str)
        parser.add_argument('path_to_csv', nargs=1, type=str)
        parser.add_argument(
            '-m',
            default='replace',
            choices=['replace', 'append'],
            help=('Define the behavior if the table already exists. '
                  'Defaults to replace mode.')
        )

    def handle(self, *args, **options):

        try:
            import pandas as pd
            from sqlalchemy import create_engine
        except ImportError as e:
            raise CommandError(str(e))

        table, *_ = options.get('table_name')
        csv, *_ = options.get('path_to_csv')
        host = settings.DATABASES['default'].get('HOST')
        port = settings.DATABASES['default'].get('PORT') or False
        user = settings.DATABASES['default'].get('USER')
        password = settings.DATABASES['default'].get('PASSWORD')
        db_name = settings.DATABASES['default'].get('NAME')
        host_port = f'{host}:{port}' if port else host

        print(f'Generating table {table} based on {csv} file')

        try:
            engine = create_engine(
                f'postgresql://{user}:{password}@{host_port}/{db_name}')

            replace = True if options['m'] == 'replace' else False
            for df in pd.read_csv(csv, chunksize=1000):
                if_exists = 'replace' if replace else 'append'
                df.to_sql(table, engine, if_exists=if_exists, index=False)
                replace = False

        except Exception as e:
            raise CommandError(str(e))
