# EFREI-M1-Big-Data-Programming-1-TP-Final
Projet Big Data programming effectué à l'efrei en M1.

## sujet: 
Vous êtes dans la peau d'un architecture data, l'équipe métier vous soumet une problématique métier (celle que vous aurez trouvé). 
Vous devez proposez une architecture permettant le traitement des données massives en s'appuyant sur une architecture en 3 couches: Datalake (bronze), Datawarehouse (silver), Datamart  (gold).  L'équipe métier veut exploiter votre objet business final à travers une API donc il faut une BDD en gold. 

Cette base de donnée pourrait également être utilisée pour faire de la visualisation.  
Pour des raisons de coûts et pour une solution tactique, vous avez décidé d'utliser Hive comme datawarehouse.
En tant qu'architecture vous tenez compte des problématiques de performance et de stockage et prenez les mesures nécessaires y afférantes.
Le métier se demande s'il est possible d'utiliser l'IA pour prédire des résultats ou faire des clusterings métiers. Il vous demande d'explorer cette piste.
 
 ## Problématique

Comment proposer des recommandations musicales personnalisées aux utilisateurs en temps réel, à partir de leurs historiques d’écoute et des caractéristiques des morceaux, en s’appuyant sur une architecture Big Data scalable ?

## Explication du projet

Ce projet vise à concevoir une architecture Big Data permettant de traiter et d’analyser des logs d’écoute musicale simulés (stream de logs), afin de construire un système de recommandation musicale personnalisé.  
Les données sources sont issues d’un simulateur de stream d’écoute, inspiré du dataset Spotify (voir [exemple Kaggle](https://www.kaggle.com/code/vatsalmavani/music-recommendation-system-using-spotify-dataset)).  
L’architecture suit le modèle 3 couches :

- **Bronze (Datalake)** : stockage brut des logs d’écoute simulés et des métadonnées des morceaux.
- **Silver (Datawarehouse/Hive)** : nettoyage, enrichissement et structuration des données via Spark et Hive.
- **Gold (Datamart)** : génération des recommandations personnalisées, exposition via une API, et préparation pour la visualisation ou l’analyse avancée (machine learning).

L’objectif final est de permettre à une application métier d’obtenir, via une API, des recommandations musicales adaptées à chaque utilisateur, tout en explorant des approches IA pour améliorer la pertinence des suggestions (clustering, prédiction…). 


## Dataset utilisé: 

https://www.kaggle.com/code/vatsalmavani/music-recommendation-system-using-spotify-dataset

