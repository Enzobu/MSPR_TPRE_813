FROM dpage/pgadmin4:latest

COPY ./config.json /pgadmin4/servers.json

ENV PGADMIN_DEFAULT_EMAIL=admin@admin.com
ENV PGADMIN_DEFAULT_PASSWORD=s5t4v5

EXPOSE 80

CMD ["/usr/local/bin/python3", "/pgadmin4/pgAdmin4.py"]