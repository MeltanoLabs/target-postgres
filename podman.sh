#/bin/bash
#Username postgres password postgres
podman run -e POSTGRES_PASSWORD=postgres -p 5432:5432 -h postgres -d postgres
