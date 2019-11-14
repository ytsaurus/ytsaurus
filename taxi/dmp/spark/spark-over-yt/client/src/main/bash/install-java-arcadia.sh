#!/bin/bash

ARC_JAVA_PATH=$(dirname $(dirname $(ya tool java --print-path)))
JAVA_HOME=/opt/jdk-8.yandex
sudo mkdir -p $JAVA_HOME && sudo rm -rf $JAVA_HOME && sudo cp -pRLP "$ARC_JAVA_PATH/" $JAVA_HOME
echo -e "export JAVA_HOME=$JAVA_HOME" >>~/.bash_profile
echo -e "export PATH=\$PATH:\$JAVA_HOME/bin" >>~/.bash_profile
echo -e "export JAVA_HOME=$JAVA_HOME" >>~/.zprofile
echo -e "export PATH=\$PATH:\$JAVA_HOME/bin" >>~/.zprofile

source ~/.bash_profile
