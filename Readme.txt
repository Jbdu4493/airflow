Examen de BIZET Jonathan Aiflow
Il y a 2 versions de l'exercice, une version un peu plus classique et une version améliorée.

Dans la version améliorée, les nœuds/tache liés à l'entraînement des modèles sont crée dynamiquement. En effet, la Variable "model_data" est une base de données des modèles que l'on souhaite entraîner.

Les donnée de modèle de "model_data" sont les suivant :

model_class_path : cette donnée permettra d'instancier le modèle via "eval".
model_name : cette donnée sera utile pour le nom de la sauvegarde du ".pickle".
model_short_name : cette donnée sera utile pour le nom des tâches et task_id.