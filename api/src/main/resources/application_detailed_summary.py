from itertools import cycle
import time
import json
import logging
import sys
from importlib import import_module
import multiprocessing

from base_summary import BaseSummary
from plugins_summary.yarn_connection import YarnConnection
import application_registrar
import application_summary_registrar
import deployer_utils

class ApplicationDetailedSummary(object):

    def __init__(self, environment, config):
        self._environment = environment
        self._configs = config
        self._application_registrar = \
        application_registrar.HbaseApplicationRegistrar(environment['hbase_thrift_server'])
        self._application_summary_registrar = \
        application_summary_registrar.HBaseAppplicationSummary(environment['hbase_thrift_server'])
        self._yarn_connection = \
        YarnConnection(environment['yarn_resource_manager_host'], \
            environment['yarn_resource_manager_port'])
        self._base_summary = BaseSummary()
        self._component_creators = {}
        self._process_matrix = {
            "process-1": {
                "status": "AVAILABLE",
                "start_time": "",
                "instance": None
            },
            "process-2": {
                "status": "AVAILABLE",
                "start_time": "",
                "instance": None
            },
            "process-3": {
                "status": "AVAILABLE",
                "start_time": "",
                "instance": None
            },
            "process-4": {
                "status": "AVAILABLE",
                "start_time": "",
                "instance": None
            }
        }

    def _update_pool(self):
        for process_name in self._process_matrix:
            if self._process_matrix[process_name]["instance"] is not None \
            and self._process_matrix[process_name]["instance"].is_alive():
                if (time.time() - self._process_matrix[process_name]["start_time"]) >= 60:
                    self._process_matrix[process_name]["instance"].terminate()
                    self._process_matrix[process_name]["status"] = "AVAILABLE"
            else:
                self._process_matrix[process_name]["status"] = "AVAILABLE"

    def generate(self):
        """
        Update applications detailed summary
        """
        applist = self._application_registrar.list_applications()
        self._application_summary_registrar.remove_app_entry(applist)
        logging.info("List of applications: %s", ', '.join(applist))
        process_pool = cycle(self._process_matrix)
        for app in applist:
            for process_name in process_pool:
                self._update_pool()
                if self._process_matrix[process_name]["status"] == "AVAILABLE":
                    process = multiprocessing.Process(name=process_name, \
                        target=self.generate_summary, args=(app,))
                    process.start()
                    self._process_matrix[process_name]["start_time"] = time.time()
                    self._process_matrix[process_name]["status"] = "BUSY"
                    self._process_matrix[process_name]["instance"] = process
                    break

    def generate_summary(self, application):
        """
        Update HBase wih recent application summary
        """
        create_data = self._application_registrar.get_create_data(application)
        input_data = {}
        for component_name, component_data in create_data.iteritems():
            input_data[component_name] = {}
            input_data[component_name]["component_ref"] = self._load_creator(component_name)
            input_data[component_name]["component_data"] = component_data
        app_data = self._base_summary.get_application_summary(application, input_data)
        logging.info("Application: %s, Status: %s", application, app_data[application]['aggregate_status'])
        self._application_summary_registrar.post_to_hbase(app_data, application)

    def _load_creator(self, component_type):

        creator = self._component_creators.get(component_type)

        if creator is None:

            cls = '%s%sSummary' % (component_type[0].upper(), component_type[1:])
            try:
                module = import_module("plugins_summary.%s" % component_type)
                self._component_creators[component_type] = getattr(module, cls)\
                (self._environment, self._yarn_connection, self._application_summary_registrar)
                creator = self._component_creators[component_type]
            except ImportError as exception:
                logging.error(
                    'Unable to load Creator for component type "%s" [%s]',
                    component_type,
                    exception)

        return creator

def main():
    """
    main
    """
    config = None
    with open('dm-config.json', 'r') as con:
        config = json.load(con)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.getLevelName(config['config']['log_level']),
                        stream=sys.stderr)

    deployer_utils.fill_hadoop_env(config['environment'], config['config'])

    summary = ApplicationDetailedSummary(config['environment'], config['config'])

    logging.info('Starting... Building actual status for applications')

    while True:
        summary.generate()
        time.sleep(10)

if __name__ == "__main__":
    main()
