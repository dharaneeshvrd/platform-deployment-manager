from plugins_summary.base_component import Component

class JupyterSummary(Component):

    def get_component_type(self):
        return 'jupyter'

    def get_component_summary(self, component):
        return {}
