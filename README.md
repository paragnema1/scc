# scc


```mermaid
graph TD;
    SCC-->Configuration_file;
    SCC-->Source_code;
    Configuration_file-->SCC.conf;
    Configuration_file-->section_connection.conf;
    Configuration_file-->section_configuration.conf;
    Configuration_file-->yard_configuration.conf;
    Source_code-->insert_conf.py;
    Source_code-->insert_yard_conf.py;
    Source_code-->main.py;
    Source_code-->scc_dlm_conf.py;
    Source_code-->scc_dlm_model.py;
    Source_code-->scc_layout_model.py;
    Source_code-->scc_trail_trough.py;
    Source_code-->trail_trough.py;
```
