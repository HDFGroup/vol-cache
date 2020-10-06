#Makefile
#!/bin/sh

build:
#	pdflatex node_local_storage_CCIO.tex
	pdflatex cache_vol.tex
	pdflatex cache_vol.tex
	bibtex cache_vol
	pdflatex cache_vol.tex
	pdflatex cache_vol.tex
clean:
	rm -f *.log *.aux *.soc *Notes.bib *.out *~ node_local_storage_CCIO.pdf *.bbl *.gz cache_vol.pdf *.blg
