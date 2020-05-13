FROM openjdk:11@sha256:736dd07fc5cb53060a49d38c86b9e673974510c5a18afc40b1c936475f59aee7

ARG TGZ_FILE

ADD $TGZ_FILE /opt/lookup

ENTRYPOINT ["java", "-cp", "/opt/lookup/bin/*:/opt/lookup/lib/*:/opt/lookup/conf/", "dewilson.projects.lookup.server.Server", "config=config", "mode=PRODUCTION"]