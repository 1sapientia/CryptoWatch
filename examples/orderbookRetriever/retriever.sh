
#!/bin/bash
# run command:     ./retriever.sh > logs.txt 2>&1 & disown
until ./orderbookRetrieverCassandra1.1; do
    echo "retriever crashed with exit code $?.  Respawning.." >&2
    sleep 10
done



