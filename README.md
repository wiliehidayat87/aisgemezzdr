SPECIFIED USER & PASSWORD FOR RABBIT :
===
1. sudo docker exec -it [rabbitmq-container-name] or [rabbitmq-container-id] /bin/bash
2. Add user => rabbitmqctl add_user adminxmp xmp2022
3. Set user as administrator => rabbitmqctl set_user_tags adminxmp administrator
4. Set permission current user for all vhost => rabbitmqctl set_permissions -p / adminxmp ".*" ".*" ".*"