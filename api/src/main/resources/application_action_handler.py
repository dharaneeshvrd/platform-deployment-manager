import json
import copy
import logging
from exceptiondef import NotFound

with open('action-details.json') as f:
    ACTION_DETAILS = json.load(f)

class ActionHandler(object):
    def __init__(self, application_registrar, application_creator, environment, dispatcher):
        self.flink_handler = FlinkActionHandler(application_registrar, application_creator, environment, dispatcher)
        self.sparkStreaming_handler = SparkStreamingActionHandler()
        self.oozie_handler = OozieActionHandler()
        self.jupyter_handler = JupyterActionHandler()

    def do_action(self, application, user_name, component_name, category, action, request_body):
        to_call = 'self.%s_handler.do_action(application, user_name, category, action, request_body)' % component_name
        return eval(to_call)

    def get_action_list(self, application, component):
        to_call = 'self.%s_handler.get_action_list(application)' % component
        return eval(to_call)

    def delete_action_data(self, application, create_data):
        for component in create_data:
            to_call = 'self.%s_handler.delete_action_data(application)' % component
            return eval(to_call)

class FlinkActionHandler(object):
    def __init__(self, application_registrar, application_creator, environment, dispatcher):
        self._application_registrar = application_registrar
        self._environment = environment
        self._application_creator = application_creator
        self.dispatcher = dispatcher

    def get_action_list(self, application):
        ret_list = []
        action_list = copy.deepcopy(ACTION_DETAILS['flink_action_list'])
        for action in action_list:
            if action['id'] in ['restore', 'dispose']: 
                savepoint_data = self._application_registrar.get_specific_record(application, 'savepoints')
                if savepoint_data:
                    for savepoint in json.loads(savepoint_data):
                        action['input']['options'].append(savepoint['path'])
            action.pop('required')
            ret_list.append(action)
        return ret_list

    def do_action(self, application, user_name, category, action, request_body):
	if category == "savepoint":
	    if action == "list":
		return self.list_savepoint(application)
	    if action == "trigger":
		self.trigger_savepoint(application, user_name)
	    if action == "restore":
		self.restore_savepoint(application, request_body)
	    if action == "dispose":
                self.dispose_savepoint(application, request_body) 

    def validate_savepoint(self, application, savepoints):
        is_savepoint_available = False
        record = json.loads(self._application_registrar.get_specific_record(application, 'savepoints'))
        for savepoint in savepoints:
            for savepoint_db_data in record:
                if savepoint == savepoint_db_data['path']:
                    is_savepoint_available = True

            if not is_savepoint_available:
                information = 'savepoint %s is not available' % savepoint
                raise NotFound(json.dumps({'information': information}))

    def list_savepoint(self, application):
        logging.debug('get_flink_savepoints')
        record = []
        savepoint_data = self._application_registrar.get_specific_record(application, 'savepoints')
        if savepoint_data:
            record = json.loads(savepoint_data)
        return record

    def trigger_savepoint(self, application, user_name):
        def do_work():
            create_data = self._application_registrar.get_create_data(application)
            savepoint_path = self._application_creator.flink_trigger_savepoint(application, create_data, user_name)
            self._application_registrar.flink_store_savepoint(application, savepoint_path)

        self.dispatcher.run_as_asynch(task=do_work)

    def restore_savepoint(self, application, savepoint_path):
        savepoint = savepoint_path['path']
        self.validate_savepoint(application, [savepoint])

        def do_work():
            status = self._application_registrar.get_application(application)['status']
            is_stopped = False
            if status == "CREATED":
                is_stopped = True
            create_data = self._application_registrar.get_create_data(application)
            self._application_creator.flink_restore_savepoint(application, create_data, savepoint, is_stopped)
            if is_stopped:
                information = '%s restored to savepoint %s' % (application, savepoint)
                self._application_registrar.set_application_status(application, "STARTED", information)

        self.dispatcher.run_as_asynch(task=do_work)

    def dispose_savepoint(self, application, savepoint_path):
        savepoints = savepoint_path['path']
        self.validate_savepoint(application, savepoints)

        def do_work():
            self._application_creator.flink_remove_savepoint(savepoints)
            self._application_registrar.flink_remove_savepoint(application, savepoints)

        self.dispatcher.run_as_asynch(task=do_work)

    def delete_action_data(self, application):
        savepoint_data = self._application_registrar.get_specific_record(application, 'savepoints')
        savepoints = []
        for savepoint in json.loads(savepoint_data):
            savepoints.append(savepoint['path'])
        self._application_creator.flink_remove_savepoint(savepoints)

class SparkStreamingActionHandler(object):
    def __init__(self):
        pass

    def get_action_list(self, application):
        return []

    def delete_action_data(self, application):
        pass

class OozieActionHandler(object):
    def __init__(self):
        pass

    def get_action_list(self, application):
        return []

    def delete_action_data(self, application):
        pass

class JupyterActionHandler(object):
    def __init__(self):
        pass

    def get_action_list(self, application):
        return []

    def delete_action_data(self, application):
        pass
