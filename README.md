# Projet Spark — Analyse & Prévision de la Pollution Urbaine (Scala)

Ce repo implémente un pipeline complet (batch + streaming optionnel) conforme au sujet :
- Ingestion/Nettoyage (CSV/JSON) avec Spark SQL/DataFrames
- Transformations fonctionnelles (map/filter/flatMap, groupBy/aggregate)
- Analyses (stats, pics, indicateur global, anomalies)
- Graphe (GraphX) : centralité + propagation simple
- Prédiction (Spark MLlib) : régression (RandomForest/GBT)

## Prérequis
- Spark 3.5.x (Scala 2.12)
- Java 11+ (ou 8 selon ton Spark)
- sbt

> Les dépendances Spark sont en `provided` : tu exécutes avec `spark-submit` (recommandé) ou via ton IDE en ajoutant SPARK_HOME.

## Structure
- `src/main/scala/urbanpollution/` : code Scala
- `data/` : petit dataset d’exemple + dossier `stream/` pour le streaming
- `reports/` : `report.tex` (rapport) + `slides.tex` (beamer)

## (A) Générer un dataset (optionnel)
Le projet contient déjà un petit dataset dans `data/`.
Tu peux regénérer un dataset plus grand :

```bash
sbt "runMain urbanpollution.DataGenerator data 30 21"
```

Arguments : (outputDir, nbStations, nbJours).

## (B) Lancer le batch job
```bash
sbt "runMain urbanpollution.BatchJob data output"
```

Sorties :
- `output/station_stats/`
- `output/peaks/`
- `output/anomalies/`
- `output/predictions/`
- `output/graph_metrics/`

## (C) Lancer le streaming (optionnel)
Terminal 1 :
```bash
sbt "runMain urbanpollution.StreamingJob data/stream output/stream"
```

Terminal 2 : copie des petits fichiers CSV vers `data/stream/` (simulation capteurs)
```bash
cp data/stream_samples/*.csv data/stream/
```

## Idées d’extensions
- Vraies données publiques (Airparif, OpenAQ, etc.)
- Features "lag" plus riches, saisonnalité, météo réelle
- Propagation basée sur direction du vent / flux passagers
