Custom bins in VIVA Stage 6

Set **`SP_EnergyBinning=1`** so the custom bins are not combined.

Set the custom bin file via `SP_BinningFilename="bins.txt" where the filename `bins.txt` must be in quotes.
The custom bin file is formatted as follows:
One number per line. This number corresponds to the _lower_ bin edge, in units of  **log10(TeV)**.
Round the edges to the hundreths place (e.g. log10(0.13745)=-0.86167... so use -0.86 in the custom bin file).

