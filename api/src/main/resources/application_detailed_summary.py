import time
import json
import logging
import sys
from importlib import import_module
from multiprocessing import TimeoutError as ThreadTimeoutError
import requests

from summary_aggregator import ComponentSummaryAggregator
from plugins_summary.yarn_connection import YarnConnection
from async_dispatcher import AsyncDispatcher
from lifecycle_states import ApplicationState
import application_registrar
import application_summary_registrar
import deployer_utils


# constants
SUMMARY_INTERVAL = 5
STATUS_INTERVAL = 0.1
REST_API_REQ_TIMEOUT = 5
MAX_SUMMARY_TIMEOUT = 60
MAX_APP_SUMMARY_TIMEOUT = 6
TRANSITION_STATES = [ApplicationState.STARTING, ApplicationState.STOPPING]

def milli_time():
    return int(round(time.time() * 1000))

class ApplicationDetailedSummary(object):

    def __init__(self, environment, config):
        self._environment = environment
        self._environment.update({'rest_api_req_timeout': REST_API_REQ_TIMEOUT})
        self._config = config
        self._application_registrar = application_registrar.HbaseApplicationRegistrar(environment['hbase_thrift_server'])
        self._application_summary_registrar = application_summary_registrar.HBaseAppplicationSummary(environment['hbase_thrift_server'])
        self._yarn_connection = YarnConnection(self._environment)
        self._summary_aggregator = ComponentSummaryAggregator()
        self._component_creators = {}
        self.dispatcher = AsyncDispatcher(num_threads=4)
        self.rest_client = requests

    def generate(self):
        """
        Every SUMMARY_INTERVAL, all applications summary will get generated and it's posted to HBase after post-processing
        """
        applist = self._application_registrar.list_applications()
        self._application_summary_registrar.sync_with_dm(applist)
        apps_to_be_processed = {}

        for app in applist:
            apps_to_be_processed.update({app: {}})
            apps_to_be_processed[app]["old_summary"] = self._application_summary_registrar.get_summary_data(app)
            apps_to_be_processed[app]["thread_ref"] = self.generate_summary(app)

        logging.info("List of submitted applications: %s", ', '.join(apps_to_be_processed.keys()))

        wait_time = 0
        apps_processed = []

        # waiting block for all the application to get completed
        while len(apps_to_be_processed) != 0:
            for app_name in apps_to_be_processed.keys():
                try:
                    new_summary = apps_to_be_processed[app_name]["thread_ref"].task.get(STATUS_INTERVAL)
                    if new_summary:
                        if new_summary != apps_to_be_processed[app_name]["old_summary"]:
                            apps_processed.append(app_name)
                            self._application_summary_registrar.post_to_hbase(new_summary, app_name)
                        self.update_dm_state(app_name, new_summary[app_name]["aggregate_status"])
                    del apps_to_be_processed[app_name]
                except ThreadTimeoutError:
                    wait_time += STATUS_INTERVAL # increasing the wait time by status interval
                    if round(wait_time, 1) % MAX_SUMMARY_TIMEOUT == 0:
                        # logging out list of applications whose wait time exceeds the max app summary timeout, on the interval of same max app summary timeout
                        # i.e. every 60 seconds as per current max app summary timeout
                        logging.error("Timeout exceeded, %s applications waiting for %d seconds", (',').join(apps_to_be_processed.keys()), int(wait_time))

        logging.info("List of processed applications: %s", ', '.join(apps_processed))

    def generate_summary(self, application):
        """
        Application detailed summary generator
        """
        def _do_generate():
            ret_data = {}
            try:
                create_data = self._application_registrar.get_create_data(application)
                input_data = {}
                for component_name, component_data in create_data.iteritems():
                    input_data[component_name] = {}
                    input_data[component_name]["component_ref"] = self._load_creator(component_name)
                    input_data[component_name]["component_data"] = component_data
                ret_data = self._summary_aggregator.get_application_summary(application, input_data)
                logging.debug("Application: %s, Status: %s", application, ret_data[application]['aggregate_status'])
            except Exception as ex:
                logging.error('%s while trying to get status of application "%s"', str(ex), application)
            return ret_data

        return self.dispatcher.run_as_asynch(task=_do_generate)

    def generate_detailed_summary(self, application, default_status):
        """
        Specific application's detailed summary will be generated and generated aggregate status will get returned
        In case of any error (thread timeout or REST timout) default status will be returned
        """
        ret_status = default_status
        summary = self.generate_summary(application)
        try:
            summary = summary.task.get(MAX_APP_SUMMARY_TIMEOUT)
            if summary:
                self._application_summary_registrar.post_to_hbase(summary, application)
                ret_status = self.get_dm_lifecycle_state(summary[application]['aggregate_status'])
        except ThreadTimeoutError:
            logging.debug("Summary generation timeout exceeded, default status will be returned for %s", application)
        return ret_status

    def get_dm_lifecycle_state(self, state):
        dm_state = ""

        if state == "STARTING" or state == "RUNNING" or state == "RUNNING_WITH_ERRORS":
            dm_state = ApplicationState.STARTED
        elif state == "STOPPED" or state == "STOPPED_WITH_FAILURES":
            dm_state = ApplicationState.STOPPED
        elif state == "COMPLETED_WITH_FAILURES":
            dm_state = ApplicationState.FAILED
        elif state == "KILLED" or state == "KILLED_WITH_FAILURES":
            dm_state = ApplicationState.KILLED
        elif state == "COMPLETED":
            dm_state = ApplicationState.COMPLETED

        return dm_state

    def update_dm_state(self, application, summary_state):
        state = self.get_dm_lifecycle_state(summary_state)
        current_dm_state = self._application_registrar.get_application(application)["status"]
        if state and current_dm_state not in TRANSITION_STATES and state != current_dm_state:
            try:
                self._application_registrar.set_application_status(application, state)
                self._state_change_event(application, state)
                logging.info("%s state changed to %s", application, state)
            except Exception as ex:
                logging.error("%s while trying to change %s state to %s", str(ex), application, state)

    def _state_change_event(self, application, state):
        endpoint_type = "application_callback"
        callback_url = self._config[endpoint_type]
        if callback_url:
            logging.debug("callback: %s %s %s", endpoint_type, application, state)
            callback_payload = {
                "data": [
                    {
                        "id": application,
                        "state": state,
                        "timestamp": milli_time()
                    }
                ],
                "timestamp": milli_time()
            }
            self.rest_client.post(callback_url, json=callback_payload)

    def _load_creator(self, component_type):

        creator = self._component_creators.get(component_type)

        if creator is None:

            cls = '%s%sComponentSummary' % (component_type[0].upper(), component_type[1:])
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
        # making sure every 30 seconds generate summary initiated
        start_time_on_cur_round = milli_time()

        summary.generate()

        finish_time_on_cur_round = (milli_time() - start_time_on_cur_round)/1000.0
        logging.info("Finished generating summary, time taken %s seconds", str(finish_time_on_cur_round))

        if finish_time_on_cur_round >= SUMMARY_INTERVAL:
            continue
        else:
            # putting sleep only for the remainig time from the current round's time
            time.sleep(SUMMARY_INTERVAL - finish_time_on_cur_round)

if __name__ == "__main__":
    main()
