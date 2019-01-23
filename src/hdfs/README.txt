Pour Martin :

À améliorer sur hdfs v2 d'après les remarques d'hagimont (pour plus de perf) :

-> La division en chunks d'un fichier par HdfsClient créée pour l'instant des blocks de taille fixe, il peut y avoir plus d'un chunk d'un même fichier sur un dataNode,
ce qui conduit à la création de plusieurs Map sur un même dataNode au lieu d'un seul.
Pour plus de performances diviser le fichier autant de fois qu'il y a de dataNodes disponibles, chunks de taille variable et donc 1 chunks par fichier par dataNode et une seule procédure Map
sur chaque dataNode.

-> Les chunks sont pour l'instant mesurés en nombres d'enregistrements (unité renvoyé par KVFormat et LineFormat) et non par leur taille en octets. Mieux vaudrait compter leur taille
en octets et les couper à la fin du l'enregistrement actuel une fois la taille voulue dépassée. Ainsi pour découper un fichier en autant de chunks qu'il y a de dataNode : récupérer la taille du
fichier en lisant les méta-données du fichier, diviser par le nombre de dataNodes puis lire enregistrement par enregistrement le fichier et construire les chunks selon la taille voulue.
