#!/bin/bash

set -e

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..

target=${1:-*}

# make sure CircleCI gets all tags properly.
# see https://discuss.circleci.com/t/where-are-my-git-tags/2371
# and https://stackoverflow.com/questions/37531605/how-to-test-if-git-repository-is-shallow
[ -f $(git rev-parse --git-dir)/shallow ] && git fetch --unshallow

source scripts/version-tag.sh

BUILDDIR=$(pwd)/build

# Make dir
mkdir -p $BUILDDIR

# Clean build bin dir
rm -rf $BUILDDIR/${target}

# disable cgo
export CGO_ENABLED=0

function fail () {
	echo "Aborting due to failure." >&2
	exit 2
}

# Build binary
cd cmd
for bin in ${target}; do
  cd $bin
  if [ "$1" == "-race" ]
  then
    set -x
    # -race requires CGO
    CGO_ENABLED=1 go build -race -ldflags "-X main.version=$version" -o $BUILDDIR/$bin || fail
  elif [ "$1" == "-debug" ]
  then
    set -x
    # -debug flags
    CGO_ENABLED=0 go build -gcflags "all=-N -l" -ldflags "-X main.version=${version}-debug" -o $BUILDDIR/$bin || fail
  else
    set -x
    go build -ldflags "-X main.version=$version" -o $BUILDDIR/$bin || fail
  fi
  set +x
  cd ..
done
