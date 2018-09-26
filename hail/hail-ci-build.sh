#!/bin/bash
set -ex

ROOT=$(cd .. && pwd)

CLUSTER_NAME=ci-test-$(LC_CTYPE=C LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 8)

time source activate hail
time pip search cloudtools
time pip install -U cloudtools
gcloud auth activate-service-account \
    hail-ci-0-1@broad-ctsa.iam.gserviceaccount.com \
    --key-file=/secrets/hail-ci-0-1.key
    
mkdir -p build

COMPILE_LOG="build/compilation-log"
SCALA_TEST_LOG="build/scala-test-log"
PYTHON_TEST_LOG="build/python-test-log"
DOCTEST_LOG="build/doctest-log"
DOCS_LOG="build/docs-log"
GCP_LOG="build/gcp-log"

COMP_SUCCESS="build/_COMP_SUCCESS"
SCALA_TEST_SUCCESS="build/_SCALA_TEST_SUCCESS"
PYTHON_TEST_SUCCESS="build/_PYTHON_TEST_SUCCESS"
DOCTEST_SUCCESS="build/_DOCTEST_SUCCESS"
DOCS_SUCCESS="build/_DOCS_SUCCESS"
GCP_SUCCESS="build/_GCP_SUCCESS"

SUCCESS='<span style="color:green;font-weight:bold">SUCCESS</span>'
FAILURE='<span style="color:red;font-weight:bold">FAILURE</span>'
SKIPPED='<span style="color:gray;font-weight:bold">SKIPPED</span>'

get_status() {
    FILE_LOC=$1
    DEPENDENCY=$2
    if [ -n "${DEPENDENCY}" ] && [ "${DEPENDENCY}" != "${SUCCESS}" ]; then
        echo ${SKIPPED};
    elif [ -e ${FILE_LOC} ]; then
        echo ${SUCCESS};
    else echo ${FAILURE};
    fi
}


on_exit() {
    trap "" INT TERM
    set +e
    ARTIFACTS=${ROOT}/artifacts
    rm -rf ${ARTIFACTS}
    mkdir -p ${ARTIFACTS}
    cp build/libs/hail-all-spark.jar ${ARTIFACTS}/hail-all-spark.jar
    cp build/distributions/hail-python.zip ${ARTIFACTS}/hail-python.zip
    cp ${COMPILE_LOG} ${ARTIFACTS}
    cp ${SCALA_TEST_LOG} ${ARTIFACTS}
    cp ${PYTHON_TEST_LOG} ${ARTIFACTS}
    cp ${DOCTEST_LOG} ${ARTIFACTS}
    cp ${GCP_LOG} ${ARTIFACTS}
    cp -R build/www ${ARTIFACTS}/www
    cp -R build/reports/tests ${ARTIFACTS}/test-report

    COMP_STATUS=$(get_status "${COMP_SUCCESS}")
    SCALA_TEST_STATUS=$(get_status "${SCALA_TEST_SUCCESS}")
    PYTHON_TEST_STATUS=$(get_status "${PYTHON_TEST_SUCCESS}" "${SCALA_TEST_STATUS}")
    DOCTEST_STATUS=$(get_status "${DOCTEST_SUCCESS}" "${PYTHON_TEST_STATUS}")
    DOCS_STATUS=$(get_status "${DOCS_SUCCESS}" "${DOCTEST_STATUS}")
    GCP_STATUS=$(get_status "${GCP_SUCCESS}")

    cat <<EOF > ${ARTIFACTS}/index.html
<html>
<head>
<style type="text/css">
    body {
        font-family: "Lucida Sans Unicode", "Lucida Grande", sans-serif;
    }
</style>
</head>
<h2>$(git rev-parse HEAD)</h2>
<h3>Compilation</h3>
<table>
<tbody>
<tr>
<td>${COMP_STATUS}</td>
<td><a href='compilation-log'>Compilation log</a></td>
</tr>
<tr>
<td>${COMP_STATUS}</td>
<td><a href='hail-all-spark.jar'>hail-all-spark.jar</a></td>
</tr>
<tr>
<td>${COMP_STATUS}</td>
<td><a href='hail-python.zip'>hail-python.zip</a></td>
</tr>
</tbody>
</table>
<h3>Tests</h3>
<table>
<tbody>
<tr>
<td>${SCALA_TEST_STATUS}</td>
<td><a href='scala-test-log'>Scala test log</a></td>
</tr>
<tr>
<td>${SCALA_TEST_STATUS}</td>
<td><a href='test-report/index.html'>TestNG report</a></td>
</tr>
<tr>
<td>${PYTHON_TEST_STATUS}</td>
<td><a href='python-test-log'>PyTest log</a></td>
</tr>
<tr>
<td>${DOCTEST_STATUS}</td>
<td><a href='doctest-log'>Doctest log</a/td>
</tr>
</tbody>
</table>
<h3>Docs</h3>
<table>
<tbody>
<tr>
<td>${DOCS_STATUS}</td>
<td><a href='docs-log'>Docs build log</a/td>
</tr>
<tr>
<td>${DOCS_STATUS}</td>
<td><a href='www/index.html'>Generated website</a></td>
</tr>
</tbody>
</table>
<h3>Cloud test</h3>
<table>
<tbody>
<tr>
<td>${GCP_STATUS}</td>
<td><a href='gcp-log'>GCP log</a></td>
</tr>
</tbody>
</table>
</html>
EOF
    time gcloud dataproc clusters delete ${CLUSTER_NAME} --async
}
trap on_exit EXIT

# some non-bash shells (in particular: dash and sh) do not trigger EXIT if the
# interpretation halts due to INT or TERM. Explicitly calling exit when INT or
# TERM is received ensures the EXIT handler is called.
trap "exit 42" INT TERM

export GRADLE_OPTS="-Xmx2048m"
export GRADLE_USER_HOME="/gradle-cache"

echo "Compiling..."
./gradlew shadowJar archiveZip > ${COMPILE_LOG}
touch ${COMP_SUCCESS}

test_project() {
    ./gradlew test > ${SCALA_TEST_LOG}
    touch ${SCALA_TEST_SUCCESS}
    ./gradlew testPython > ${PYTHON_TEST_LOG}
    touch ${PYTHON_TEST_SUCCESS}
    ./gradlew doctest > ${DOCTEST_LOG}
    touch ${DOCTEST_SUCCESS}
    ./gradlew makeDocs > ${DOCS_LOG}
    touch ${DOCS_SUCCESS}
}

test_gcp() {
    time gsutil cp \
         build/libs/hail-all-spark.jar \
         gs://hail-ci-0-1/temp/$SOURCE_SHA/$TARGET_SHA/hail.jar

    time gsutil cp \
         build/distributions/hail-python.zip \
         gs://hail-ci-0-1/temp/$SOURCE_SHA/$TARGET_SHA/hail.zip

    time cluster start ${CLUSTER_NAME} \
         --version devel \
         --spark 2.2.0 \
         --max-idle 40m \
         --bucket=hail-ci-0-1-dataproc-staging-bucket \
         --jar gs://hail-ci-0-1/temp/$SOURCE_SHA/$TARGET_SHA/hail.jar \
         --zip gs://hail-ci-0-1/temp/$SOURCE_SHA/$TARGET_SHA/hail.zip \
         --vep

    time cluster submit ${CLUSTER_NAME} \
         cluster-sanity-check.py

    time cluster submit ${CLUSTER_NAME} \
         cluster-vep-check.py

    time cluster stop ${CLUSTER_NAME} --async
    touch ${GCP_SUCCESS}
}

test_project

test_gcp > ${GCP_LOG}
