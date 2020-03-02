# Build from the PredictiveAnalyticsPython directory:
#    docker build -f .\zielgruppen.dockerfile -t test/zielgruppen .
#
# Run (replace "C:\Users\kpf\data" by the localtion of PA_DATA_DIR on the host computer):
#    docker run --rm -p 8080:8080 -v C:\Users\kpf\data:/root/data --name zg test/zielgruppen
#
# View web app from local container:
#    http://localhost:8080
#
# Pull and run centralized version from docker hub (e.g. on lxewi041):
#    docker pull kpflugshaupt/zielgruppen
#    docker run --rm -p 8080:8080 -v /home/pa/data:/root/data --name zg kpflugshaupt/zielgruppen
#
# View web app (e.g. from lxewi041):
#    http://lxewi041.apgsga.ch:8080
#
# Connect into shell:
#    docker exec -it zg bash
#
# Stop container:
#    docker stop zg
#

# prepare Python environment
FROM python:3
COPY zielgruppen_requirements.txt ./
RUN pip install --no-cache-dir -r zielgruppen_requirements.txt

# copy code files
WORKDIR /app
COPY axinova/__init__.py axinova/Uplift* axinova/*_analyse.py ./axinova/
COPY pa_lib ./pa_lib

# prepare volume for source data files
VOLUME /root/data

# run streamlit server
EXPOSE 8080
WORKDIR /app/axinova
CMD ["streamlit", "run", "allianz_analyse.py", "--server.port", "8080"]
