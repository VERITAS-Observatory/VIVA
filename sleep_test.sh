#!/bin/bash

TIME=$1
echo "Sleeping for $TIME seconds..."
sleep $TIME
rv=$?
echo "Time to wake up!"
exit $rv
