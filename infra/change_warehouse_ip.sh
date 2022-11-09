warehouse=infra_db_1

DB_IP=$(docker inspect $warehouse | jq .[].NetworkSettings.Networks.infra_default.IPAddress | sed -e 's/^.//' -e 's/.$//')
echo "The IP Address of $warehouse container is $DB_IP"
echo "Changing the warehouse_ip variables in file airflow_variables/warehouse.json"

tmp=airflow_variables/temp.json
jq --arg a "$DB_IP" '.warehouse_ip = $a' airflow_variables/warehouse.json > "$tmp" && mv "$tmp" airflow_variables/warehouse.json