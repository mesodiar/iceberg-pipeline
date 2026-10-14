# Match the launcher and worker Python versions. Build this image for linux/amd64.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest AS launcher
FROM apache/beam_python3.11_sdk:2.77.0

COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
WORKDIR /template
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir '.[gcp]' && pip check
COPY docker/launch.py /template/launch.py
COPY setup.py /template/setup.py
COPY docker/requirements-dataflow.txt /template/requirements-dataflow.txt
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/launch.py
# Stage our package and its cloud dependencies to the default Beam SDK worker image.
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/setup.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements-dataflow.txt
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
