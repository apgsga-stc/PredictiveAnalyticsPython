# Build (must be in PredictiveAnalyticsPython directory):
#    docker build -f .\zielgruppen.dockerfile -t test/zielgruppen .
#
# Run locally (replace "C:\Users\kpf\data" by the location of PA_DATA_DIR on your host computer):
#    docker run -it --rm -p 8080:8080 -p 8081:8081 -v C:\Users\kpf\data:/root/data --name zg test/zielgruppen
#
# View web app from local container:
#    http://localhost:8080
# View export directory:
#    http://localhost:8081
#
# Build server version and push to docker hub
#    docker build -f .\zielgruppen.dockerfile -t kpflugshaupt/zielgruppen .
#    docker push kpflugshaupt/zielgruppen
#
# Pull and run server version from docker hub (e.g. on lxewi041. Replace ZG_HOST_NAME if different, e.g. with CLZHG552433283.affichage-p.ch):
#    docker pull kpflugshaupt/zielgruppen
#    docker run -d --rm -p 8080:8080 -p 8081:8081 --env ZG_HOST_NAME='lxewi041.apgsga.ch' -v /home/pa/data:/root/data --name zg kpflugshaupt/zielgruppen
#
# View web app (e.g. from lxewi041):
#    http://lxewi041.apgsga.ch:8080
# View export directory:
#    http://lxewi041.apgsga.ch:8081
#
# Connect into shell for debugging:
#    docker exec -it zg bash
#
# Stop container:
#    docker stop zg
#

# prepare Python environment
FROM python:3.8-slim
COPY zielgruppen_requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/zielgruppen_requirements.txt

# server parameters
ARG server_script_dir="/app/axinova"
ARG server_script="allianz_analyse.py"
ARG server_port=8080
ARG src_data_dir="/root/data"
ARG export_dir="/app/output"
ARG export_port=8081
# command to run file server (on output directory) and streamlit server
ARG run_cmd="python -m http.server ${export_port} --directory ${export_dir} &  \
             streamlit run ${server_script} --server.port ${server_port}"

# copy code files into container
WORKDIR /
COPY axinova/__init__.py axinova/Uplift* axinova/*_analyse.py ${server_script_dir}/
COPY pa_lib /app/pa_lib

# prepare volume for source data files
VOLUME ${src_data_dir}

# create output directory for XLSX downloads, set env vars for script
RUN mkdir -p ${export_dir}
ENV ZG_EXPORT_DIR=${export_dir}
ENV ZG_EXPORT_PORT=${export_port}

# default hostname, must be overridden in run command for remote download to work!
ENV ZG_HOST_NAME="localhost"

# flag ports for mapping (must be done in run command)
EXPOSE ${server_port}
EXPOSE ${export_port}

# copy dynamically prepared CMD into image env, run it from there
ENV RUN_CMD=${run_cmd}
WORKDIR ${server_script_dir}
CMD /bin/bash -c "${RUN_CMD}"
