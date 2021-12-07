echo "@softwareversion{bracco:hal-03450104v1,
  TITLE = {Baromètre lorrain de la Science Ouverte},
  AUTHOR = {Bracco, Laetitia},
  URL = {https://hal.univ-lorraine.fr/hal-03450104},
  NOTE = {},
  INSTITUTION = {Université de Lorraine},
  YEAR = {2020},
  MONTH = Jun,
  SWHID = {swh:1:dir:ac6f5993fb42654e1d4d6df167c8b081e7e96394;origin=https://hal.archives-ouvertes.fr/hal-03450104;visit=swh:1:snp:9362b8d55dc613dd98ec95acf30e784ebd8e8692;anchor=swh:1:rel:077ee6a62016e3bf376c47b9e6a9fb476dcbe4e2;path=/},
  REPOSITORY = {https://gitlab.com/Cthulhus_Queen/barometre_scienceouverte_universitedelorraine},
  LICENSE = {Apache License 2.0},
  KEYWORDS = {Science ouverte ; Bibliométrie},
  FILE = {https://hal.univ-lorraine.fr/hal-03450104/file/barometre_scienceouverte_universitedelorraine-master.zip},
  HAL_ID = {hal-03450104},
  HAL_VERSION = {v1},
}" >> bso.bib
docker run --rm -v "$(pwd):/data" -u "$(id -u)" pandocscholar/alpine
cp out.pdf bso.pdf
cp out.latex bso.tex
