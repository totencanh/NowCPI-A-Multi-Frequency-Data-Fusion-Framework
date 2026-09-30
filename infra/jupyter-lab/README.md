# NowCPI JupyterLab

The JupyterLab image and Spark configuration are scaffolded here, but
JupyterLab is not currently a service in `docker-compose.yaml`. The old
`make up-explore` instruction does not apply to this project. Add a Compose
service when an interactive notebook environment is needed.

The Spark defaults use the NowCPI MinIO and Hive Metastore settings; credentials
are supplied through environment variables rather than stored in this file.
