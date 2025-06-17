docker run --rm -v "$(pwd):/data" -u "$(id -u)" pandocscholar/alpine
cp out.pdf worksmagnet.pdf
cp out.latex worksmagnet.tex
