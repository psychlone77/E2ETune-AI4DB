cd /home/benchbase/target/benchbase-$1
java -jar benchbase.jar -b $2 -c /home/E2ETune-AI4DB/oltp_workloads/tpcc/sample_tpcc_config0.xml --create=true --load=true --execute=false