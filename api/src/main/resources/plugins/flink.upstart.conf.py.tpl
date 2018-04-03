start on runlevel [2345]
stop on runlevel [016]
${respawn}
${respawn_limit}
setuid ${application_user}
env FLINK_VERSION=${component_flink_version}
pre-start exec /opt/${environment_namespace}/${component_application}/${component_name}/flink-cancel.py
pre-stop exec /opt/${environment_namespace}/${component_application}/${component_name}/flink-cancel.py
chdir /opt/${environment_namespace}/${component_application}/${component_name}/
exec flink run -m yarn-cluster ${component_flink_config_args} -ynm ${component_job_name} -v ${flink_python_jar} ${component_main_py} ${component_application_args}
