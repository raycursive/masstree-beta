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
COLLECT_KEYS = ["puts_per_sec", "gets_per_sec", "ops_per_sec"]


def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-dsl", "--data-structure-list",
        dest="ds_list",
        action="store",
        required=True
    )

    parser.add_argument(
        "-tc", "--threads-count",
        dest="threads_count",
        action="store",
        required=True
    )

    parser.add_argument(
        "-tl", "--tests-list",
        dest="tests_list",
        action="store",
        required=True
    )

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

    return parser


class ReportGenerator:
    def __init__(self):
        self.__res = defaultdict(lambda: defaultdict(list))  # Key - (ds, test), Value - list of runs (threads)

    def parse_res_line(self, line):
        if ':' not in line:
            return

        possible_res = line.split(':', 1)[1].strip()
        if possible_res[0] != '{':
            return

        res_json = json.loads(possible_res)
        self.__res[res_json["test"]][res_json["table"]].append(res_json)

    def store_results(self, file_path):
        with open(file_path, 'w') as handle:
            handle.write(json.dumps(self.__res))

    def plot_results(self, test_id, results_dir, threads_count):
        for test_name in self.__res:
            data_structs = []
            data = {k: [] for k in COLLECT_KEYS}

            for ds, rows in self.__res[test_name].items():
                data_structs.append(ds)

                for k in COLLECT_KEYS:
                    data[k].append(statistics.mean([row[k] for row in rows]) / 1000)  # Kilo ops

            res_fig_path = os.path.join(results_dir, f'fig_{test_id}.png')
            title = f"Test {test_name} with {threads_count} threads"
            self.__plot_res(title, data_structs, data, res_fig_path)

    @staticmethod
    def __plot_res(title, data_structs, data, res_fig_path):
        x = np.arange(len(data_structs))  # the label locations
        width = 0.25  # the width of the bars
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained')
        max_measurement = 0

        for attribute, measurement in data.items():
            offset = width * multiplier
            rects = ax.bar(x + offset, measurement, width, label=attribute)
            max_measurement = max(max_measurement, max(measurement))
            ax.bar_label(rects, padding=3)
            multiplier += 1

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('Throughput per thread per second (K)')
        ax.set_title(title)
        ax.set_xticks(x + width, data_structs)
        ax.legend(loc='upper left')
        ax.set_ylim(0, max_measurement + 1000)

        plt.savefig(res_fig_path)


def execute_and_collect_data(test_id, ds, threads: int, tests_list, results_dir, report_gen: ReportGenerator):
    command = COMMAND_FORMAT.format(ds=ds, threads=threads, tests_list=shlex.quote(tests_list))
    res_file_path = os.path.join(results_dir, f"raw_{test_id}.txt")

    print("Executing:", command)
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    with open(res_file_path, 'a') as handle:
        for line in proc.stderr:
            line = line.decode('utf-8')
            handle.write(line)
            report_gen.parse_res_line(line)
            print(line.rstrip())  # See output

        handle.flush()

    print("Done")


def handle_cli():
    parser = create_parser()
    args = parser.parse_args()
    results_base = os.path.join(args.results_dir, args.test_id)
    os.makedirs(results_base, exist_ok=True)

    ds_list = args.ds_list.split(',')
    threads_count = int(args.threads_count)  # Converting to make sure it's integer
    report_gen = ReportGenerator()

    for ds in ds_list:
        execute_and_collect_data(args.test_id, ds, threads_count, args.tests_list, results_base, report_gen)

    res_json_file_path = os.path.join(results_base, f"results_{args.test_id}.json")
    report_gen.store_results(res_json_file_path)
    report_gen.plot_results(args.test_id, results_base, threads_count)


if __name__ == "__main__":
    handle_cli()
