#/bin/bash
#pip3 install -r /opt/spark/work-dir/collector/requirements.txt
python3 /opt/spark/work-dir/collector/main.py

# set up spark tables. this would be a seperate piece of code that maanages the create and table properties of a table
# base on configs
/opt/spark/bin/spark-submit --properties-file /opt/spark/work-dir/processors/spark.properties /opt/spark/work-dir/processors/table_manager.py
# runs our bronze table landing append from source collector (json)
/opt/spark/bin/spark-submit --properties-file /opt/spark/work-dir/processors/spark.properties /opt/spark/work-dir/processors/bronze_processor.py

/opt/spark/bin/spark-submit --properties-file /opt/spark/work-dir/processors/spark.properties /opt/spark/work-dir/processors/silver_processor.py

/opt/spark/bin/spark-submit --properties-file /opt/spark/work-dir/processors/spark.properties /opt/spark/work-dir/report/report.py

#/opt/spark/bin/pyspark --properties-file /opt/spark/work-dir/processors/spark.properties