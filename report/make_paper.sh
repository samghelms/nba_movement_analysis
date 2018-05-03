# pandoc -t latex -s arch_280_paper.md > arch_280_paper.tex

pandoc --toc --standalone ./report.md -s -o report.tex

../../../tectonic/target/debug/tectonic ./report.tex
