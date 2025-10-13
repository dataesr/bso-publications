docker run --rm -v "$(pwd):/data" -u "$(id -u)" pandocscholar/alpine
mv out.pdf dataset.pdf
mv out.latex dataset.tex
