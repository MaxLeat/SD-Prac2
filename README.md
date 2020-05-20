# SD-Prac2
 Pràctica 2 de Sistemes Distribuits de la Universitat Rovira i Virgili
 
 ## Funció del programa
La funcionalitat del programa es garantir l'exclusió mutua. 

Es crea un slave que té la funció d'administrar quan els slaves poden editar un fitxer de text, aquests slaves esperen a que el master els hi doni permís per poder modificar l'arxiu, un cop l'han modificat acaben la seva execució. El master espera que tots els slaves hagin acabat la seva funció i retorna una llista de com hauria d'estar el fitxer moddificat per tots els slaves.

Posteriorment al main es comprova si el fitxer té el mateix contingut que el que ha retornat la funció master.
