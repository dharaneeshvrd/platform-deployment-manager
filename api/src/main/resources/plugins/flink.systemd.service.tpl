[Unit]
Description=PNDA Application: ${component_application}-${component_name}

[Service]
Type=simple
User=${application_user}
EnvironmentFile=${environment_file_path}
WorkingDirectory=/opt/${environment_namespace}/${component_application}/${component_name}/
ExecStartPre=/opt/${environment_namespace}/${component_application}/${component_name}/flink-stop.py '${dynamic_component_job_name}'
ExecStop=/opt/${environment_namespace}/${component_application}/${component_name}/flink-stop.py '${dynamic_component_job_name}'
Environment=FLINK_VERSION=${component_flink_version}
ExecStart=${environment_flink} run -m  yarn-cluster ${component_flink_config_args} -ynm ${dynamic_component_job_name} --class ${component_main_class} ${component_main_jar} ${component_application_args}
Restart=${component_respawn_type}
RestartSec=${component_respawn_timeout_sec}
