import argparse
import os
import sys
from sys import argv
from datalakeingestion.jobs.etl import JobDataLakeIngestion

parser = argparse.ArgumentParser(description='Process datalake ingestion')
parser.add_argument('-j', '--job',
                    type=str,
                    required=True,
                    default=None,
                    help='The name of the job you want to execute')


def functions(argument):
    switcher = {
        'JobDataLakeIngestion': JobDataLakeIngestion
    }
    func = switcher.get(argument, lambda: "nothing")
    return func()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--job',
                        type=str,
                        required=True,
                        default=None,
                        help='The name of the job you want to run')

    args = parser.parse_args()
    job = argv[0]

    functions(job)

    pex_file = os.path.basename([path for path in sys.path if path.endswith('.pex')][0])
    os.environ['PYSPARK_PYTHON'] = "./" + pex_file

    dataingestion = JobDataLakeIngestion(pex_file)
    dataingestion.run()


if __name__ == "__main__":
    main()
