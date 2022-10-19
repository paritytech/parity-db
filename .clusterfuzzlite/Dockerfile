FROM gcr.io/oss-fuzz-base/base-builder-rust:v1
COPY . $SRC/parity-db
WORKDIR parity-db
COPY .clusterfuzzlite/build.sh $SRC/
