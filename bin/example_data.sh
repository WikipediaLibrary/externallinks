#!/bin/bash

echo "Creating Programs"
python /app/manage.py programs_example_data 10
echo "Creating LinkSearchTotals"
python /app/manage.py linksearchtotal_example_data 60
echo "Creating LinkEvents"
python /app/manage.py linkevent_example_data 10000