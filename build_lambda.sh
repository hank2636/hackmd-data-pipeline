#!/bin/bash
set -e

PYTHON_VERSION=3.11
VENV_DIR=".venv"
LAYER_DIR="layer"
LAMBDA_LAYER_ZIP="lambda_layer.zip"
HANDLER_PARENT_DIR="lambda_handlers"

rm -rf $LAYER_DIR
rm -f $LAMBDA_LAYER_ZIP
rm -f lambda_handler_*.zip

mkdir -p $LAYER_DIR/python/lib/python$PYTHON_VERSION/site-packages

EXCLUDE_PKGS="boto3 botocore s3transfer urllib3 fastapi uvicorn prometheus_client pytest black moto starlette werkzeug h11 anyio coverage _pytest requests-aws4auth"
for pkg in $(ls $VENV_DIR/lib/python$PYTHON_VERSION/site-packages); do
    skip=false
    for ex in $EXCLUDE_PKGS; do
        if [[ "$pkg" == "$ex"* ]]; then
            skip=true
            break
        fi
    done
    if [ "$skip" = false ]; then
        cp -r $VENV_DIR/lib/python$PYTHON_VERSION/site-packages/$pkg $LAYER_DIR/python/lib/python$PYTHON_VERSION/site-packages/
    fi
done

find $LAYER_DIR -name "__pycache__" -type d -exec rm -rf {} +
find $LAYER_DIR -name "*.pyc" -type f -delete
find $LAYER_DIR -name "*.dist-info" -type d -exec rm -rf {} +

cd $LAYER_DIR
zip -r9q ../$LAMBDA_LAYER_ZIP .
cd ..

for dir in $HANDLER_PARENT_DIR/*; do
    if [ -d "$dir" ]; then
        name=$(basename $dir)
        HANDLER_ZIP="lambda_handler_${name}.zip"
        rm -rf package_handler_tmp
        mkdir -p package_handler_tmp
        cp -r $dir/* package_handler_tmp/
        cp -r src package_handler_tmp/
        cd package_handler_tmp
        zip -r9q ../$HANDLER_ZIP .
        cd ..
        rm -rf package_handler_tmp
        echo "$name zip: $HANDLER_ZIP"
    fi
done

