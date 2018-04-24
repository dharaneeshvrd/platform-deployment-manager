import commands

from plugins_summary.base_component import Component

class Common(Component):

    def get_component_summary(self, component):

        ret_data = {}
        check_in_service = False
        status, timestamp = \
        self._application_summary_registrar.get_status_with_timestamp(self._application_name)

        job_name = component['component_job_name']
        component_name = component['component_name']
        (aggregate_status, yarnid, tracking_url, information) = ('', '', '', '')
        yarn_data = self._yarn_connection.check_in_yarn(job_name)
        if status == 'CREATED':
            if yarn_data != None:
                aggregate_status, yarnid, tracking_url, information = self.yarn_handler(yarn_data)
            else:
                aggregate_status = "CREATED"
        else:
            if yarn_data != None:
                if timestamp < yarn_data['startedTime']:
                    aggregate_status, yarnid, tracking_url, information = \
                    self.yarn_handler(yarn_data)
                else:
                    check_in_service = True
            else:
                check_in_service = True
        if check_in_service:
            aggregate_status, information = self.check_in_service_log(self.environment['namespace']\
                , self._application_name, component_name)
        ret_data = {
            'aggregate_status': aggregate_status,
            'yarnId': yarnid,
            'tracking_url': tracking_url,
            'information': information,
            'name': job_name,
            'componentType': "%s%s" % (self._component_type[0].upper(), self._component_type[1:])
        }

        return ret_data

    def check_in_service_log(self, namespace, application, component_name):
        '''
        Check in service log in case of application failed to submit to YARN
        '''
        service_name = '%s-%s-%s' % (namespace, application, component_name)
        (command, message, more_detail) = ('', '', '')
        command = 'sudo journalctl -u %s.service' % service_name
        out = commands.getoutput('%s -n 50' % command).split('\n')
        more_detail = 'More details: execute "journalctl -u %s"' % service_name

        for line in out:
            if 'Exception:' in line:
                message = '%s%s' % (line.split('Exception:')[0].split(' ')[-1], 'Exception')
                break

        if message == '':
            message = '%s' % (more_detail)
        else:
            message = '%s. %s' % (message, more_detail)

        return 'FAILED_TO_SUBMIT_TO_YARN', message
