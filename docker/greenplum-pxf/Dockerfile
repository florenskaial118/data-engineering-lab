FROM datagrip/greenplum:6.8

USER root

COPY pxf-mock/server.py /opt/pxf-mock/server.py

CMD python3 /opt/pxf-mock/server.py & \
    /etc/init.d/ssh start && \
    su - gpadmin bash -c /home/gpadmin/install_and_start_gpdb.sh && \
    su - gpadmin bash -c /entrypoint.sh && \
    tail -f /dev/null
