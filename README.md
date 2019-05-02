# Projet Verteego
Ce projet utilise un fichier CSV de données: population homme et population femme, par pays.
À partir du fichier CSV, un fichier parquet est crée pour cet exercice.

Ensuite, le fichier est chargé en mémoire, quelques petites stats sont calculées (population totale, pourcentage femme / homme), les top X pays en terme de population et de pourcentage sont montrés, et les données sont sauvées dans un nouveau fichier parquet.

# Dépendences
Ce projet utilise la librairie disponible à https://github.com/xitongsys/parquet-go pour lire et écrire des fichiers parquet.

# Exécution
Soit en mode complet:
```sh
go build
go install
$GOPATH/bin/verteego
```

Soit en mode instantané:
```
go run *.go
```
