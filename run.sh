#!/bin/bash
echo "-----Extract script running..."
# Run the extract Python file
python -u ./scripts/extract.py

# Check if the extract script ran successfully
if [ $? -eq 0 ]; then
    echo "-----Transform script running..."
    # Run the transform Python file
    python -u ./scripts/transform.py

    # Check if the transform script ran successfully
    if [ $? -eq 0 ]; then
        echo "-----SQL queries running..."
        # Run the SQL file query using SQLite
        python -u ./scripts/run_queries.py
    else
        echo "Transform script failed. SQL query will not run."
    fi
else
    echo "Extract script failed. Transform script will not run."
fi