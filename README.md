### Dremio dbt Exporter
This project will help you export an existing Dremio environment to a dbt project quickly


### Setup config.ini
Setup your config.ini file uisng the following for Dremio Cloud
``` 
[Dremio Cloud]
host = api.dremio.cloud
port = 443
ssl = true
type = cloud
username =
password =
project_id =
output = project
```
Setup your config.ini file uisng the following for Dremio Software

```
[Dremio Software]
host = localhost
port = 9047
ssl = false
type = software
username = dremio
password = dremio123
output = project
```

### Running
1. Install dbt dremio `pip install dbt-dremio`
2. Initiate your dbt project `dbt init <project_name>`
3. Select `dremio` as the database to use.

python main.py config.ini target

- arguments
  - config path
  - target in the config to use

### Current features

- export tables and views in Dremio to a models directory
- update dbt_project.yml with Dremio schema