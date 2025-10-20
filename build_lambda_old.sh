#!/bin/bash
set -e

# ----------------------------
PYTHON_VERSION=3.11
VENV_DIR=".venv"
LAYER_DIR="layer"
HANDLER_DIR="package_handler"
LAMBDA_LAYER_ZIP="lambda_layer.zip"
LAMBDA_HANDLER_ZIP="lambda_handler.zip"
HANDLER_SRC="lambda_handlers"
# ----------------------------

echo "=== 清理舊的目錄與 zip ==="
rm -rf $LAYER_DIR $HANDLER_DIR
rm -f $LAMBDA_LAYER_ZIP $LAMBDA_HANDLER_ZIP

echo "=== 建立 Layer 目錄結構 ==="
mkdir -p $LAYER_DIR/python/lib/python$PYTHON_VERSION/site-packages

echo "=== 複製依賴到 Layer（排除多餘套件） ==="
EXCLUDE_PKGS="boto3 botocore s3transfer urllib3 fastapi uvicorn prometheus_client pytest black moto starlette werkzeug h11 anyio coverage _pytest requests-aws4auth"
for pkg in $(ls $VENV_DIR/lib/python$PYTHON_VERSION/site-packages); do
    skip=false
    for ex in $EXCLUDE_PKGS; do
        if [[ "$pkg" == "$ex"* ]]; then
            echo "跳過已排除套件: $pkg"
            skip=true
            break
        fi
    done
    if [ "$skip" = false ]; then
        cp -r $VENV_DIR/lib/python$PYTHON_VERSION/site-packages/$pkg $LAYER_DIR/python/lib/python$PYTHON_VERSION/site-packages/
    fi
done

echo "=== 移除不必要檔案 ==="
find $LAYER_DIR -name "__pycache__" -type d -exec rm -rf {} +
find $LAYER_DIR -name "*.pyc" -type f -delete
find $LAYER_DIR -name "*.dist-info" -type d -exec rm -rf {} +

echo "=== 打包 Layer zip ==="
cd $LAYER_DIR
zip -r9q ../$LAMBDA_LAYER_ZIP .
cd ..

echo "=== 建立 Handler 目錄 ==="
mkdir -p $HANDLER_DIR
echo "=== 複製 Handler 程式碼 ==="
cp $HANDLER_SRC/*.py $HANDLER_DIR/
cp -r src $HANDLER_DIR/

echo "=== 打包 Handler zip ==="
cd $HANDLER_DIR
zip -r9q ../$LAMBDA_HANDLER_ZIP .
cd ..

echo "=== 完成 ==="
echo "Layer zip: $LAMBDA_LAYER_ZIP"
echo "Handler zip: $LAMBDA_HANDLER_ZIP"

