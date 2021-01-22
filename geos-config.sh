#!/bin/sh

while test $# -gt 0; do
  case "$1" in
    -*=*) optarg=`echo "$1" | sed 's/[-_a-zA-Z0-9]*=//'` ;;
    *) optarg= ;;
  esac
  case $1 in
    --ldflags)
      echo $LDFLAGS $LIBS
      ;;
    --includes)
      echo $(grep -A999 "\[includedirs\]" conanbuildinfo.txt | grep -m1 geos)
      ;;
  esac
  shift
done
