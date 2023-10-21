import argparse
import datetime
import json
import os
import shlex
import subprocess
from collections import defaultdict
import statistics
import numpy as np
from matplotlib import pyplot as plt

COMMAND_FORMAT = "./simpletest -j{threads} -p {ds} {tests_list}"
MT_TEST_COMMAND_FORMAT = "./mttest -j{threads} {tests_list}"
COLLECT_KEYS = ["puts_per_sec", "gets_per_sec", "ops_per_sec"]


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser.add_argument(
        "-tid", "--test-id",
        dest="test_id",
        action="store",
        required=False,
        default=str(round(int(datetime.datetime.utcnow().timestamp())))
    )

    parser.add_argument(
        "-rdir", "--results-dir",
        dest="results_dir",
        action="store",
        required=False,
        default="./test_results"
    )

    # Test parser
    test_parser = subparsers.add_parser("test")

    test_parser.add_argument(
        "-dsl", "--data-structure-list",
        dest="ds_list",
        action="store",
        required=True
    )

    test_parser.add_argument(
        "-tl", "--tests-list",
        dest="tests_list",
        action="store",
        required=True
    )

    # Plot parser
    plot_parser = subparsers.add_parser("plot")

    plot_parser.add_argument(
        "-s", "--source",
        dest="source",
        action="store",
        required=True
    )

    return parser


class ReportGenerator:
    def __init__(self):
        self.__res = defaultdict(lambda: defaultdict(list))  # Key - (ds, test), Value - list of runs (threads)

    def load_from_file(self, file_path):
        with open(file_path) as handle:
            self.__res = json.loads(handle.read())

    def parse_res_line(self, line, ds):
        if ':' not in line:
            return

        possible_res = line.split(':', 1)[1].strip()
        if possible_res[0] != '{':
            return

        res_json = json.loads(possible_res)
        self.__res[res_json["test"]][ds].append(res_json)

    def store_results(self, file_path):
        with open(file_path, 'w') as handle:
            handle.write(json.dumps(self.__res))

    def plot_results(self, res_fig_path):
        for test_name in self.__res:
            data_structs = []
            data = {k: [] for k in COLLECT_KEYS}

            for ds, rows in self.__res[test_name].items():
                data_structs.append(ds)

                for k in COLLECT_KEYS:
                    data[k].append(round(statistics.mean([row[k] for row in rows]), 2))

            title = f"Test {test_name}"
            self.__plot_res(title, data_structs, data, res_fig_path)

    @staticmethod
    def __plot_res(title, data_structs, data, res_fig_path):
        x = np.arange(len(data_structs))  # the label locations
        width = 0.25  # the width of the bars
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained')
        # max_measurement = 0

        for attribute, measurement in data.items():
            offset = width * multiplier
            plot_measurements = [round(m / 1e6, 2) for m in measurement]  # Million ops rounded to 2 decimals
            rects = ax.bar(x + offset, plot_measurements, width, label=attribute)
            # max_measurement = max(max_measurement, max(measurement))
            ax.bar_label(rects, padding=3, rotation=60)
            multiplier += 1

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('Throughput per thread per second (in millions)')
        ax.set_title(title)
        ax.set_xticks(x + width, data_structs)
        ax.legend(loc='upper left')
        # ax.set_ylim(0, max_measurement + 2)
        ax.set_ylim(0, 2)

        plt.table(cellText=list(data.values()), cellLoc="center", rowLabels=COLLECT_KEYS, bbox=[0.0, -0.45, 1, .28])
        fig.set_size_inches(8, 6)
        plt.savefig(res_fig_path, bbox_inches='tight', dpi=100)


def get_command(ds, tests_list):
    ds_name, threads = ds.split(':')
    assert threads.isnumeric()
    tests_list = shlex.quote(tests_list)

    if ds_name == "masstree":
        return MT_TEST_COMMAND_FORMAT.format(threads=threads, tests_list=tests_list)
    else:
        return COMMAND_FORMAT.format(ds=ds_name, threads=threads, tests_list=tests_list)


def execute_and_collect_data(test_id, ds: str, tests_list, results_dir, report_gen: ReportGenerator):
    """
    :param ds: <ds_name>:<threads_cnt>
    """
    res_file_path = os.path.join(results_dir, f"raw_{test_id}.txt")
    command = get_command(ds, tests_list)

    print("Executing:", command)
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    with open(res_file_path, 'a') as handle:
        for line in proc.stderr:
            line = line.decode('utf-8')
            handle.write(line)
            report_gen.parse_res_line(line, ds)
            print(line.rstrip())  # See output

        handle.flush()

    print("Done")


def handle_test(args):
    results_base = os.path.join(args.results_dir, args.test_id)
    os.makedirs(results_base, exist_ok=True)

    ds_list = args.ds_list.split(',')
    report_gen = ReportGenerator()

    for ds in ds_list:
        execute_and_collect_data(args.test_id, ds, args.tests_list, results_base, report_gen)

    res_json_file_path = os.path.join(results_base, f"results_{args.test_id}.json")
    res_fig_path = os.path.join(results_base, f'fig_{args.test_id}.png')
    report_gen.store_results(res_json_file_path)
    report_gen.plot_results(res_fig_path)


def handle_plot(args):
    report_gen = ReportGenerator()
    report_gen.load_from_file(args.source)

    results_base = os.path.join(args.results_dir, args.test_id)
    os.makedirs(results_base, exist_ok=True)
    res_fig_path = os.path.join(results_base, f'fig_{args.test_id}.png')
    report_gen.plot_results(res_fig_path)


def handle_cli():
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "test":
        handle_test(args)
    else:
        handle_plot(args)


if __name__ == "__main__":
    handle_cli()
