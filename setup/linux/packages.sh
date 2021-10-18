#!/bin/bash

echo 'Downloading Ubuntu packages for Spark'

sudo apt-get install -y python3.8 \
                        python3.8-venv \
                        python3-pip \
                        default-jre \
                        scala
                    
echo 'Done!'
