import json
import unittest
from multiprocessing import Event
from mock import patch, Mock
from requests.exceptions import Timeout

from application_detailed_summary import ApplicationDetailedSummary
from summary_aggregator import ComponentSummaryAggregator
from application_registrar import HbaseApplicationRegistrar
from application_summary_registrar import HBaseAppplicationSummary

class ApplicationDetailedSummaryTests(unittest.TestCase):
    def setUp(self):
        self.mock_environment = {
            'hbase_thrift_server': 'hbasehost',
            'yarn_resource_manager_host': '1.1.1.1',
            'yarn_resource_manager_port': '8888',
            'flink_history_server': 'flink_host:8082',
            'oozie_uri': 'oozie_host:11000/oozie',
            'namespace': 'platform-app'}
        self.mock_config = {"application_callback": "edge:3001/apps"}

    @patch('requests.get')
    @patch('happybase.Connection')
    def test_sparkstreaming_component(self, mock_hbase, mock_get_requests):
        # SparkStreaming CREATED status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"sparkStreaming": [{"component_name": "example", \
            "component_job_name": "app1-example-job"}]}'},
            {'cf:status': 'CREATED'}]
        mock_get_requests.return_value = type('obj', (object,), {
            'status_code' : 200,
            'text': json.dumps({
                "apps": {"app": []}})})
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        worker = app_summary.generate_summary("app1")
        on_complete.wait(1)
        expected_summary = {
            'app1': {
                'aggregate_status': 'CREATED',
                'sparkStreaming-1': {
                    'information': '',
                    'name': u'app1-example-job',
                    'yarnId': '',
                    'componentType': 'SparkStreaming',
                    'aggregate_status': 'CREATED',
                    'tracking_url': ''}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # SparkStreaming RUNNING status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"sparkStreaming": [{"component_name": "example", \
            "component_job_name": "app1-example-job"}]}'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app1-example-job",
                        "id": "application_1234",
                        "state": "RUNNING",
                        "startedTime": 5,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'SUCCEEDED',
                    'stageIds': [
                        0,
                        1], 'jobId': 0}])}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'COMPLETE',
                    'stageId': 1}, {
                        'status': 'COMPLETE',
                        'stageId': 1}])})]
        on_complete = Event()
        worker = app_summary.generate_summary("app1")
        on_complete.wait(1)
        expected_summary = {
            'app1': {
                'aggregate_status': 'RUNNING',
                'sparkStreaming-1':{
                    'information': {
                        'stageSummary': {
                            'active': 0,
                            'number_of_stages': 2,
                            'complete': 2,
                            'pending': 0,
                            'failed': 0},
                        'jobSummary': {
                            'unknown': 0,
                            'number_of_jobs': 1,
                            'running': 0,
                            'succeeded': 1,
                            'failed': 0}},
                    'name': u'app1-example-job',
                    'yarnId': u'application_1234',
                    'tracking_url': u'xyz',
                    'componentType': 'SparkStreaming',
                    'aggregate_status': 'RUNNING'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # SparkStreaming RUNNING_WITH_ERRORS status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"sparkStreaming": [{"component_name": "example", \
            "component_job_name": "app1-example-job"}]}'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app1-example-job",
                        "id": "application_1234",
                        "state": "RUNNING",
                        "startedTime": 1512647193214,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'FAILED',
                    'stageIds': [
                        2,
                        3], 'jobId': 1}, {
                            'status': 'SUCCEEDED',
                            'stageIds': [
                                0,
                                1], 'jobId': 0}])}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([{
                'status': 'FAILED',
                'stageId': 3}, {
                    'status': 'FAILED',
                    'stageId': 2}, {
                        'status': 'COMPLETE',
                        'stageId': 1}, {
                            'status': 'COMPLETE',
                            'stageId': 0}])})]
        on_complete = Event()
        worker = app_summary.generate_summary("app1")
        on_complete.wait(1)
        expected_summary = {
            'app1': {
                'aggregate_status': 'RUNNING_WITH_ERRORS',
                'sparkStreaming-1': {
                    'information': {
                        'stageSummary': {
                            'active': 0,
                            'number_of_stages': 4,
                            'complete': 2,
                            'pending': 0,
                            'failed': 2},
                        'jobSummary': {
                            'unknown': 0,
                            'number_of_jobs': 2,
                            'running': 0,
                            'succeeded': 1,
                            'failed': 1}},
                    'name': u'app1-example-job',
                    'yarnId': u'application_1234',
                    'componentType': 'SparkStreaming',
                    'aggregate_status': 'RUNNING_WITH_ERRORS',
                    'tracking_url': u'xyz'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # SparkStreaming KILLED status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"sparkStreaming": [{"component_name": "example", \
            "component_job_name": "app1-example-job"}]}'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app1-example-job",
                        "id": "application_1234",
                        "state": "KILLED",
                        "startedTime": 5,
                        'tracking_url': u'xyz',
                        "diagnostics": "Application killed by user."}]}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app1")
        on_complete.wait(1)
        expected_summary = {
            'app1': {
                'aggregate_status': 'KILLED',
                'sparkStreaming-1': {
                    'information': u'Application killed by user.',
                    'name': u'app1-example-job',
                    'yarnId': u'application_1234',
                    'componentType': 'SparkStreaming',
                    'aggregate_status': u'KILLED',
                    'tracking_url': ''}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

    @patch('requests.get')
    @patch('happybase.Connection')
    def test_flink_component(self, mock_hbase, mock_get_requests):
        # Flink CREATED status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'CREATED'}]
        mock_get_requests.return_value = type('obj', (object,), {
            'status_code' : 200,
            'text': json.dumps({
                "apps": {"app": []}})})
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'CREATED',
                'flink-1': {
                    'information': '',
                    'name': u'app2-example-job',
                    'yarnId': '',
                    'componentType': 'Flink',
                    'aggregate_status': 'CREATED',
                    'tracking_url': ''}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Flink RUNNING status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app2-example-job",
                        "id": "application_1235",
                        "state": "RUNNING",
                        "startedTime": 5,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jobs-running': ['jhfi48y8rfuf3ci'], 'jobs-finished': []})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jid': 'jhfi48y8rfuf3ci', 'vertices': [{
                    'name': 'vertice_name',
                    'status': 'RUNNING'}]})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'RUNNING',
                'flink-1': {
                    'information': {
                        'state': 'OK',
                        'vertices': [{'status': u'RUNNING', 'name': u'vertice_name'}],
                        'flinkJid': u'jhfi48y8rfuf3ci'},
                    'name': u'app2-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': 'RUNNING',
                    'tracking_url': u'xyz#/jobs/jhfi48y8rfuf3ci'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Flink RUNNING_WITH_ERRORS status
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app2-example-job",
                        "id": "application_1235",
                        "state": "RUNNING",
                        "startedTime": 5,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jobs-running': ['jhfi48y8rfuf3ci'], 'jobs-finished': []})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jid': 'jhfi48y8rfuf3ci', 'vertices': [{
                    'name': 'vertice_name',
                    'status': 'FAILED'}]})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'RUNNING_WITH_ERRORS',
                'flink-1': {
                    'information': {
                        'state': 'ERROR',
                        'vertices': [{'status': u'FAILED', 'name': u'vertice_name'}],
                        'flinkJid': u'jhfi48y8rfuf3ci'},
                    'name': u'app2-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': 'RUNNING_WITH_ERRORS',
                    'tracking_url': u'xyz#/jobs/jhfi48y8rfuf3ci'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Flink FINISHED_SUCCEEDED state for job ran more than a minute
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'STARTED'},
            {'cf:component_data': '{"flink-1": {"tracking_url": "xyz/#/jobs/jhfi48y8rfuf3ci"}}'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app2-example-job",
                        "id": "application_1235",
                        "state": "FINISHED",
                        "finalStatus": "SUCCEEDED",
                        "startedTime": 5,
                        "diagnostics": "",
                        'tracking_url': u'xyz'}]}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'COMPLETED',
                'flink-1': {
                    'information': u'',
                    'name': u'app2-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': u'FINISHED_SUCCEEDED',
                    'tracking_url': u'http://flink_host:8082/#/jobs/jhfi48y8rfuf3ci'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Flink FINISHED_SUCCEEDED state for job less than a minute
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'STARTED'},
            {'cf:component_data': '{"flink-1": {"tracking_url": "xyz/#/jobs/jhfi48y8rfuf3ci"}}'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app2-example-job",
                        "id": "application_1235",
                        "state": "FINISHED",
                        "finalStatus": "SUCCEEDED",
                        "startedTime": 5,
                        "diagnostics": "",
                        'tracking_url': u'xyz'}]}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'COMPLETED',
                'flink-1': {
                    'information': u'',
                    'name': u'app2-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': u'FINISHED_SUCCEEDED',
                    'tracking_url': u'http://flink_host:8082/#/jobs/jhfi48y8rfuf3ci'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Flink FAILED state
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'STARTED'},
            {'cf:component_data': '{"flink-1": {"tracking_url": "xyz/#/jobs/jhfi48y8rfuf3ci"}}'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app2-example-job",
                        "id": "application_1235",
                        "state": "FAILED",
                        "finalStatus": "FAILED",
                        "startedTime": 5,
                        "diagnostics": "Failed Reason",
                        'tracking_url': u'xyz'}]}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app2")
        on_complete.wait(1)
        expected_summary = {
            'app2': {
                'aggregate_status': 'COMPLETED_WITH_FAILURES',
                'flink-1': {
                    'information': u'Failed Reason',
                    'name': u'app2-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': u'FAILED',
                    'tracking_url': u'http://flink_host:8082/#/jobs/jhfi48y8rfuf3ci'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

    @patch('requests.get')
    @patch('happybase.Connection')
    def test_oozie_component(self, mock_hbase, mock_get_requests):
        # Oozie coordinator CREATED status
        mock_hbase.return_value.table.return_value.row.return_value = {'cf:create_data': \
        '{"oozie": [{"job_handle": "123-oozie-oozi-C"}]}'}
        mock_get_requests.return_value = type('obj', (object,), {
            'status_code' : 200,
            'text': json.dumps({
                'coordJobId': '123-oozie-oozi-C',
                'coordJobName': 'app3-coordinator',
                'status': 'PREPSUSPENDED'})})
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        worker = app_summary.generate_summary("app3")
        on_complete.wait(1)
        expected_summary = {
            'app3': {
                'aggregate_status': 'CREATED',
                'oozie-1': {
                    'componentType': 'Oozie',
                    'aggregate_status': 'CREATED',
                    'name': 'app3-coordinator',
                    'oozieId': '123-oozie-oozi-C'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Oozie coordinator RUNNING state
        mock_hbase.return_value.table.return_value.row.return_value = {
            'cf:create_data': '{"oozie": [{"job_handle": "123-oozie-oozi-C"}]}'
        }
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'coordJobId': '123-oozie-oozi-C',
                'coordJobName': 'app3-coordinator',
                'actions': [{
                    'externalId': '123-oozie-oozi-W',
                    'type': None,
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'SUCCEEDED',
                'id': '123-oozie-oozi-W',
                'appName': 'app3-workflow',
                'actions': [{
                    'externalId': '124-oozie-oozi-W',
                    'type': 'sub-workflow',
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'SUCCEEDED',
                'id': '124-oozie-oozi-W',
                'appName': 'app3-subworkflow',
                'actions': [{
                    'name': 'download',
                    'status': 'OK',
                    'type': 'shell',
                    'externalId': 'job_123',
                    'externalChildIDs': None
                }, {
                    'name': 'process',
                    'status': 'OK',
                    'type': 'spark',
                    'externalId': 'job_124',
                    'externalChildIDs': 'job_125'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 5,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 6,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 7,
                    "diagnostics": "",
                    "applicationType": "SPARK"}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app3")
        on_complete.wait(1)
        expected_summary = {
            'app3': {
                'aggregate_status': 'RUNNING',
                'oozie-1': {
                    'status': 'OK',
                    'componentType':
                    'Oozie', 'oozieId': u'123-oozie-oozi-C',
                    'aggregate_status': 'RUNNING',
                    'actions': {
                        'workflow-1': {
                            'status': 'OK',
                            'oozieId': u'123-oozie-oozi-W',
                            'actions': {
                                'subworkflow-1': {
                                    'status': 'OK',
                                    'oozieId': u'124-oozie-oozi-W',
                                    'actions': {
                                        'job-2': {
                                            'status': 'OK',
                                            'information': '',
                                            'applicationType': u'spark',
                                            'name': u'process',
                                            'yarnId': u'application_125'},
                                        'job-1': {
                                            'status': 'OK',
                                            'information': '',
                                            'applicationType': u'shell',
                                            'name': u'download',
                                            'yarnId': u'application_123'}},
                                    'name': u'app3-subworkflow'}},
                            'name': u'app3-workflow'}},
                    'name': u'app3-coordinator'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Oozie coordinator RUNNING_WITH_ERRORS state
        mock_hbase.return_value.table.return_value.row.return_value = {
            'cf:create_data': '{"oozie": [{"job_handle": "123-oozie-oozi-C"}]}'}
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'coordJobId': '123-oozie-oozi-C',
                'coordJobName': 'app3-coordinator',
                'actions': [{
                    'externalId': '123-oozie-oozi-W',
                    'type': None,
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'id': '123-oozie-oozi-W',
                'appName': 'app3-workflow',
                'actions': [{
                    'externalId': '124-oozie-oozi-W',
                    'type': 'sub-workflow',
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'id': '124-oozie-oozi-W',
                'appName': 'app3-subworkflow',
                'actions': [{
                    'name': 'download',
                    'status': 'OK',
                    'type': 'shell',
                    'externalId': 'job_123',
                    'externalChildIDs': None}, {
                        'name': 'process',
                        'status': 'ERROR',
                        'type': 'spark',
                        'externalId': 'job_124',
                        'externalChildIDs': None,
                        'errorMessage': 'Pre-Launcher error'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 5,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FAILED",
                    "finalStatus": "FAILED",
                    "startedTime": 6,
                    "diagnostics": "Failed reason",
                    "applicationType": "MAPREDUCE"}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app3")
        on_complete.wait(1)
        expected_summary = {
            'app3': {
                'aggregate_status': 'RUNNING_WITH_ERRORS',
                'oozie-1': {
                    'status': 'WARN',
                    'componentType': 'Oozie',
                    'oozieId': u'123-oozie-oozi-C',
                    'aggregate_status': 'RUNNING_WITH_ERRORS',
                    'actions': {
                        'workflow-1': {
                            'status': 'WARN',
                            'oozieId': u'123-oozie-oozi-W',
                            'actions': {
                                'subworkflow-1': {
                                    'status': 'WARN',
                                    'oozieId': u'124-oozie-oozi-W',
                                    'actions': {
                                        'job-2': {
                                            'status': 'ERROR',
                                            'information': u'Failed reason, Pre-Launcher error',
                                            'applicationType': u'spark',
                                            'name': u'process',
                                            'yarnId': u'application_124'},
                                        'job-1': {
                                            'status': 'OK',
                                            'information': '',
                                            'applicationType': u'shell',
                                            'name': u'download',
                                            'yarnId': u'application_123'}},
                                    'name': u'app3-subworkflow'}},
                            'name': u'app3-workflow'}},
                    'name': u'app3-coordinator'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Oozie coordinator SUSPENDED state
        mock_hbase.return_value.table.return_value.row.return_value = {
            'cf:create_data': '{"oozie": [{"job_handle": "123-oozie-oozi-C"}]}'}
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'SUSPENDED',
                'coordJobId': '123-oozie-oozi-C',
                'coordJobName': 'app3-coordinator',
                'actions': [{
                    'externalId': '123-oozie-oozi-W',
                    'type': None,
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'SUCCEEDED',
                'id': '123-oozie-oozi-W',
                'appName': 'app3-workflow',
                'actions': [{
                    'externalId': '124-oozie-oozi-W',
                    'type': 'sub-workflow',
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'SUCCEEDED',
                'id': '124-oozie-oozi-W',
                'appName': 'app3-subworkflow',
                'actions': [{
                    'name': 'download',
                    'status': 'OK',
                    'type': 'shell',
                    'externalId': 'job_123',
                    'externalChildIDs': None}, {
                        'name': 'process',
                        'status': 'OK',
                        'type': 'spark',
                        'externalId': 'job_124',
                        'externalChildIDs': 'job_125'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 5,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 6,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FINISHED",
                    "finalStatus": "SUCCEEDED",
                    "startedTime": 7,
                    "diagnostics": "",
                    "applicationType": "SPARK"}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app3")
        on_complete.wait(1)
        expected_summary = {
            'app3': {
                'aggregate_status': 'STOPPED',
                'oozie-1': {
                    'status': 'OK',
                    'componentType': 'Oozie',
                    'oozieId': u'123-oozie-oozi-C',
                    'aggregate_status': u'SUSPENDED',
                    'actions': {
                        'workflow-1': {
                            'status': 'OK',
                            'oozieId': u'123-oozie-oozi-W',
                            'actions': {
                                'subworkflow-1': {
                                    'status': 'OK',
                                    'oozieId': u'124-oozie-oozi-W',
                                    'actions': {
                                        'job-2': {
                                            'status': 'OK',
                                            'information': '',
                                            'applicationType': u'spark',
                                            'name': u'process',
                                            'yarnId': u'application_125'},
                                        'job-1': {
                                            'status': 'OK',
                                            'information': '',
                                            'applicationType': u'shell',
                                            'name': u'download',
                                            'yarnId': u'application_123'}},
                                    'name': u'app3-subworkflow'}},
                            'name': u'app3-workflow'}},
                    'name': u'app3-coordinator'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

        # Oozie workflow COMPLTED_WITH_FAILURES state
        mock_hbase.return_value.table.return_value.row.return_value = {
            'cf:create_data': '{"oozie": [{"job_handle": "123-oozie-oozi-C"}]}'}
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'DONEWITHERROR',
                'id': '123-oozie-oozi-W',
                'appName': 'app4-workflow',
                'actions': [{
                    'externalId': '124-oozie-oozi-W',
                    'type': 'sub-workflow',
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'FAILED',
                'id': '124-oozie-oozi-W',
                'appName': 'app4-subworkflow',
                'actions': [{
                    'name': 'process',
                    'status': 'ERROR',
                    'type': 'spark',
                    'externalId': 'job_123',
                    'externalChildIDs': 'None',
                    'errorMessage': 'Pre-Launcher error'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "FAILED",
                    "finalStatus": "FAILED",
                    "startedTime": 6,
                    "diagnostics": "Failed Reason",
                    "applicationType": "MAPREDUCE"}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app4")
        on_complete.wait(1)
        expected_summary = {
            'app4': {
                'aggregate_status': 'COMPLETED_WITH_FAILURES',
                'oozie-1': {
                    'status': 'ERROR',
                    'componentType': 'Oozie',
                    'oozieId': u'123-oozie-oozi-W',
                    'aggregate_status': 'COMPLETED_WITH_FAILURES',
                    'actions': {
                        'subworkflow-1': {
                            'status': 'ERROR',
                            'oozieId': u'124-oozie-oozi-W',
                            'actions': {
                                'job-1': {
                                    'status': 'ERROR',
                                    'information': u'Failed Reason, Pre-Launcher error',
                                    'applicationType': u'spark',
                                    'name': u'process',
                                    'yarnId': 'application'}},
                            'name': u'app4-subworkflow'}},
                    'name': u'app4-workflow'}}}
        result = worker.task.get()
        self.assertEquals(result, expected_summary)

# pylint: disable=C0301
    @patch('commands.getoutput')
    @patch('requests.get')
    @patch('happybase.Connection')
    def test_check_in_service_log(self, mock_hbase, mock_get_requests, mock_command_out):
        # Testing check_in_service_log
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"sparkStreaming": [{"component_name": "example", \
            "component_job_name": "app5-example-job"}]}'},
            {'cf:status': 'STARTED'}
        ]
        mock_get_requests.return_value = type('obj', (object,), {
            'status_code' : 200,
            'text': json.dumps({
                "apps": {
                    "app": []}})})

        mock_command_out.return_value = 'Mar 22 03:39:32 rhel-cdh-hadoop-edge spark-submit[2475]: 18/03/22 03:39:32 INFO yarn.Client: Uploading resource file:/opt/platform_app/s1/example/dataplatform-raw.avsc -> hdfs://rhel-cdh-hadoop-mgr-1:8020/user/pnda/.sparkStaging/application_1521689436801_0013/dataplatform-raw.avsc\nMar 22 03:39:32 rhel-cdh-hadoop-edge spark-submit[2475]: 18/03/22 03:39:32 INFO yarn.Client: Uploading resource file:/opt/platform_app/s1/example/avro-1.8.1-py2.7.egg -> hdfs://rhel-cdh-hadoop-mgr-1:8020/user/pnda/.sparkStaging/application_1521689436801_0013/avro-1.8.1-py2.7.egg\nMar 22 03:39:32 rhel-cdh-hadoop-edge spark-submit[2475]: 18/03/22 03:39:32 INFO yarn.Client: Deleting staging directory .sparkStaging/application_1521689436801_0013\nMar 22 03:39:32 rhel-cdh-hadoop-edge spark-submit[2475]: Exception in thread "main" java.io.FileNotFoundException: File file:/opt/platform_app/s1/example/avro-1.8.1-py2.7.egg does not exist\nMar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus(RawLocalFileSystem.java:598)\nMar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.RawLocalFileSystem.getFileLinkStatusInternal(RawLocalFileSystem.java:811)\nMar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.RawLocalFileSystem.getFileStatus(RawLocalFileSystem.java:588)\nMar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:425)\nMar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:340)Mar 22 03:38:01 rhel-cdh-hadoop-edge spark-submit[31045]: at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:292)'
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        worker = app_summary.generate_summary("app5")
        on_complete.wait(1)
        expected_summary = {'app5': {'aggregate_status': 'STARTING', 'sparkStreaming-1': {'information': u'java.io.FileNotFoundException. More details: execute "journalctl -u platform-app-app5-example"', 'name': u'app5-example-job', 'yarnId': '', 'componentType': 'SparkStreaming', 'aggregate_status': 'SUBMITTING_TO_YARN', 'tracking_url': ''}}}
        result = worker.task.get()
        print result
        self.assertEquals(result, expected_summary)

    @patch('requests.get')
    @patch('happybase.Connection')
    def test_combined_component(self, mock_hbase, mock_get_requests):
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example1", \
            "component_job_name": "app6-example1-job"}], "sparkStreaming": [{"component_name": "example2", \
            "component_job_name": "app6-example2-job"}]}'},
            {'cf:status': 'STARTED'},
            {'cf:status': 'STARTED'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app6-example1-job",
                        "id": "application_1235",
                        "state": "RUNNING",
                        "startedTime": 5,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jobs-running': ['jhfi48y8rfuf3ci'], 'jobs-finished': []})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'jid': 'jhfi48y8rfuf3ci', 'vertices': [{
                    'name': 'vertice_name',
                    'status': 'FAILED'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app6-example2-job",
                        "id": "application_1234",
                        "state": "RUNNING",
                        "startedTime": 5,
                        'trackingUrl': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'SUCCEEDED',
                    'stageIds': [
                        0,
                        1], 'jobId': 0}])}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'COMPLETE',
                    'stageId': 1}, {
                        'status': 'COMPLETE',
                        'stageId': 1}])})]
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        worker = app_summary.generate_summary("app6")
        on_complete.wait(1)
        result = worker.task.get()
        self.assertEquals(result, {
            'app6': {
                'aggregate_status': 'RUNNING_WITH_ERRORS',
                'flink-1': {
                    'information': {
                        'state': 'ERROR',
                        'vertices': [{
                            'status': u'FAILED',
                            'name': u'vertice_name'}],
                        'flinkJid': u'jhfi48y8rfuf3ci'},
                    'name': u'app6-example1-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': 'RUNNING_WITH_ERRORS',
                    'tracking_url': u'xyz#/jobs/jhfi48y8rfuf3ci'},
                'sparkStreaming-1': {
                    'information': {
                        'stageSummary': {
                            'active': 0,
                            'number_of_stages': 2,
                            'complete': 2,
                            'pending': 0,
                            'failed': 0},
                        'jobSummary': {
                            'unknown': 0,
                            'number_of_jobs': 1,
                            'running': 0,
                            'succeeded': 1,
                            'failed': 0}},
                    'name': u'app6-example2-job',
                    'yarnId': u'application_1234',
                    'componentType': 'SparkStreaming',
                    'aggregate_status': 'RUNNING',
                    'tracking_url': u'xyz'}}})
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app6-example-job"}], "oozie": [{"job_handle": "123-oozie-oozi-C"}]}'},
            {'cf:status': 'STARTED'},
            {'cf:component_data': '{"flink-1": {"tracking_url": "xyz/#/jobs/jhfi48y8rfuf3ci"}}'}]
        mock_get_requests.side_effect = [
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "apps": {
                    "app": [{
                        "name": "app6-example-job",
                        "id": "application_1235",
                        "state": "FAILED",
                        "finalStatus": "FAILED",
                        "startedTime": 5,
                        "diagnostics": "Failed Reason",
                        'tracking_url': u'xyz'}]}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'id': '123-oozie-oozi-W',
                'appName': 'app6-workflow',
                'actions': [{
                    'externalId': '124-oozie-oozi-W',
                    'type': 'sub-workflow',
                    'status': 'OK'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'status': 'RUNNING',
                'id': '124-oozie-oozi-W',
                'appName': 'app6-subworkflow',
                'actions': [{
                    'name': 'process',
                    'status': 'OK',
                    'type': 'spark',
                    'externalId': 'job_123',
                    'externalChildIDs': 'job_124'}]})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "RUNNING",
                    "finalStatus": "UNDEFINED",
                    "startedTime": 6,
                    "diagnostics": "",
                    "applicationType": "MAPREDUCE"}})}),
            type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                "app": {
                    "state": "RUNNING",
                    "finalStatus": "UNDEFINED",
                    "startedTime": 7,
                    "diagnostics": "",
                    "applicationType": "SPARK"}})})]
        on_complete = Event()
        worker = app_summary.generate_summary("app6")
        on_complete.wait(1)
        result = worker.task.get()
        self.assertEquals(result, {
            'app6': {
                'aggregate_status': 'COMPLETED_WITH_FAILURES',
                'flink-1': {
                    'information': u'Failed Reason',
                    'name': u'app6-example-job',
                    'yarnId': u'application_1235',
                    'componentType': 'Flink',
                    'aggregate_status': u'FAILED',
                    'tracking_url': u'http://flink_host:8082/#/jobs/jhfi48y8rfuf3ci'},
                'oozie-1': {
                    'status': 'OK',
                    'componentType': 'Oozie',
                    'oozieId': u'123-oozie-oozi-W',
                    'aggregate_status': 'RUNNING',
                    'actions': {
                        'subworkflow-1': {
                            'status': 'OK',
                            'oozieId': u'124-oozie-oozi-W',
                            'actions': {
                                'job-1': {
                                    'status': 'OK',
                                    'information': '',
                                    'applicationType': u'spark',
                                    'name': u'process',
                                    'yarnId': u'application_124'}},
                            'name': u'app6-subworkflow'}},
                    'name': u'app6-workflow'}}})

    @patch('requests.get')
    @patch('happybase.Connection')
    def test_get_summary_status_errors(self, mock_hbase, mock_get_requests):
        # on exceptions or timeouts get_summary_status should return the default status passed to the method
        mock_hbase.return_value.table.return_value.row.side_effect = [
            {'cf:create_data': '{"flink": [{"component_name": "example", \
            "component_job_name": "app2-example-job"}]}'},
            {'cf:status': 'CREATED'}]
        mock_get_requests.side_effect = Mock(side_effect=Timeout("Requested URL got timed out"))
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        default_status = 'STOPPED'
        on_complete = Event()
        ret_status = app_summary.generate_detailed_summary('app1', default_status)
        on_complete.wait(1)
        self.assertEquals(ret_status, default_status)

    @patch.object(HbaseApplicationRegistrar, 'get_application')
    @patch.object(ComponentSummaryAggregator, 'get_application_summary')
    @patch.object(HbaseApplicationRegistrar, 'get_create_data')
    @patch.object(HBaseAppplicationSummary, 'get_summary_data')
    @patch.object(HbaseApplicationRegistrar, 'list_applications')
    @patch('time.time')
    @patch('requests.post')
    @patch('happybase.Connection')
    def test_callback_on_state_change(self, mock_hbase, mock_req_post, mock_time, mock_app_list, mock_get_summary, \
                                      mock_get_create_data, mock_generate_summary, mock_get_app):
        app_name = 'app'
        generated_summary = {'app': {'aggregate_status': 'KILLED', 'flink-1': {}}}
        mock_app_list.return_value = [app_name]
        mock_get_summary.return_value = {app_name:{'aggregate_status': 'RUNNING', 'component-1': {}}}
        mock_get_create_data.return_value = {"flink": [{"component_name": "example", "component_job_name": "app-example-job"}]}
        mock_generate_summary.return_value = generated_summary
        mock_get_app.return_value = {'status': 'STARTED'}
        mock_time.return_value = 12345.123
        app_summary = ApplicationDetailedSummary(self.mock_environment, self.mock_config)
        on_complete = Event()
        app_summary.generate()
        on_complete.wait(1)
        expected_callback_payload = {'timestamp': 12345123, 'data': [{'timestamp': 12345123, 'state': 'KILLED', 'id': 'app'}]}
        self.assertEquals(2, mock_hbase.return_value.table.return_value.put.call_count)
        mock_req_post.assert_called_with(self.mock_config['application_callback'], json=expected_callback_payload)
