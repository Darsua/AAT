#!/bin/bash
JS='const s=db.getMongo().startSession(); const c=s.getDatabase("t").c; try{ db.getSiblingDB("t").createCollection("c"); }catch(e){}
try{ s.startTransaction(); c.insertOne({a:1}); c.insertOne({a:2}); s.commitTransaction(); print("TX_OK"); }catch(e){ print("TX_FAIL: "+e.codeName+" | "+e.message); }'
wait_up(){ for i in $(seq 1 60); do docker exec "$1" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1 && return 0; sleep 1; done; return 1; }
docker rm -f mtx_a mtx_b >/dev/null 2>&1
echo "== A: standalone mongod"
docker run -d --name mtx_a mongo:7.0 >/dev/null
wait_up mtx_a && docker exec mtx_a mongosh --quiet --eval "$JS"
echo "== B: --replSet rs0, before rs.initiate()"
docker run -d --name mtx_b mongo:7.0 --replSet rs0 --bind_ip_all >/dev/null
wait_up mtx_b && docker exec mtx_b mongosh --quiet --eval "$JS"
echo "== C: single-member replica set after rs.initiate()"
docker exec mtx_b mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]}).ok'
for i in $(seq 1 30); do docker exec mtx_b mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true && break; sleep 1; done
docker exec mtx_b mongosh --quiet --eval "$JS"
docker rm -f mtx_a mtx_b >/dev/null 2>&1
echo done
