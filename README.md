# Projet Verteego
Ce projet utilise un fichier CSV de donnees: population homme, femme, par pays.
A partir du fichier CSV, un fichier parquet est cree pour cet exercice.

Ensuite, le fichier est charge en memoire, quelques petites stats sont calculees (population totale, pourcentage femme / homme), les top X pays en terme de population et de pourcentage sont montres, et les donnees sont sauves dans un nouveau fichier parquet.

# Dependences
Ce projet utilise la librairie disponible a https://github.com/xitongsys/parquet-go pour lire et ecrire des fichiers parquet.

# Execution
Soit en mode complet:
```go build
```go install
```$GOPATH/bin/verteego

Soit en mode instantane:
```go run *.go
