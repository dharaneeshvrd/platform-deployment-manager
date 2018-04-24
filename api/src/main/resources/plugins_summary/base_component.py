class Component(object):

    def __init__(self, environment, yarn_con, app_registrar):
        self.environment = environment
        self._yarn_connection = yarn_con
        self._application_summary_registrar = app_registrar
        self.component_status = dict([("green", "OK"), ("amber", "WARN"), ("red", "ERROR")])
        self._application_name = ''
        self._component_type = ''

    def get_components_summary(self, application, component_data):
        self._application_name = application
        self._component_type = self.get_component_type()
        ret_data = {}

        for count, component in enumerate(component_data):
            ret_data.update({"%s-%d" % (self._component_type, count+1): self.get_component_summary(component)})

        return ret_data
