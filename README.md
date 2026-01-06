# Projet Spark — Analyse & Prévision de la Pollution Urbaine (Scala)

Ce repo implémente un pipeline complet (batch + streaming ) conforme au sujet :
- Ingestion/Nettoyage (CSV/JSON) avec Spark SQL/DataFrames
- Transformations fonctionnelles (map/filter/flatMap, groupBy/aggregate)
- Analyses (stats, pics, indicateur global, anomalies)
- Graphe (GraphX) : centralité + propagation simple
- Prédiction (Spark MLlib) : régression (RandomForest/GBT)

## Prérequis
- Spark 3.5.x (Scala 2.12)
- Java 11+ (ou 8 selon ton Spark)
- sbt


## Structure
- `src/main/scala/urbanpollution/` : code Scala
- `data/` : petit dataset d’exemple + dossier `stream/` pour le streaming

## (A) Générer un dataset (optionnel)
Le projet contient déjà un petit dataset dans `data/`.
Tu peux regénérer un dataset plus grand :

```bash
sbt "runMain urbanpollution.DataGenerator data 30 21"
```

Arguments : (outputDir, nbStations, nbJours).

## (B) Lancer le batch job
### Build le project pour générer le jar 
```bash
sbt clean package
```
### Run 
```bash
spark-submit \
  --class urbanpollution.BatchJob \
  --master "local[*]" \
  target/scala-2.12/urban-pollution-spark_2.12-0.1.0.jar \
  data output
```

Sorties :
- `output/station_stats/`
- `output/peaks/`
- `output/anomalies/`
- `output/predictions/`
- `output/graph_metrics/`

## (C) Lancer le streaming
Terminal 1 :
### Build le project pour générer le jar 
```bash
sbt clean package
```
### Run 
```bash
spark-submit \
  --class urbanpollution.StreamingJob \
  --master "local[*]" \
  target/scala-2.12/urban-pollution-spark_2.12-0.1.0.jar \
  data/stream output/stream
```

Terminal 2 : copie des petits fichiers CSV vers `data/stream/` (simulation capteurs)
```bash
cp data/stream_samples/*.csv data/stream/
```

## Idées d’extensions
- Vraies données publiques (Airparif, OpenAQ, etc.)
- Features "lag" plus riches, saisonnalité, météo réelle
- Propagation basée sur direction du vent / flux passagers
