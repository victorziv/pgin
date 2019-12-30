#!/bin/bash

APP=${1:-tarzan}
ROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENVDIR="$ROOTDIR/venv${APP}"
# _________________________

install_modules() {
    cd ${ROOTDIR}
    source ${VENVDIR}/bin/activate
    cp "${ROOTDIR}/pip.conf" "${VENVDIR}/"
    pip install -r ${ROOTDIR}/requirements.txt
    deactivate
}
# _________________________

set_environment() {
    echo "export PYTHONPATH=${ROOTDIR}:${ROOTDIR}/${APP}" >> ${VENVDIR}/bin/activate
    echo "export PROJECT=${APP}" >> ${VENVDIR}/bin/activate
}
# _________________________

main() {
    rm -rf $VENVDIR
    virtualenv --python $(which python3.6) --no-site-packages --clear --verbose $VENVDIR
    set_environment
    install_modules
}
# _________________________

main "$@"
